"""
Сервис подбора аналогов материалов для тендерных закупок.
Ищет аналоги в локальной БД и через Gemini AI с Google Search.
"""

import asyncio
import copy
import html
import json
import logging
import re
import time
from typing import Any, Optional, Sequence
from urllib.parse import urljoin, urlparse, urlunparse
import requests
from sqlalchemy.orm import Session

logger = logging.getLogger("LegalAI")


class AnalogService:
    """
    Сервис подбора аналогов.
    Источники: локальная БД products, Gemini AI + Google Search.
    """

    def __init__(self, ai_service, db_session_factory):
        self.ai_service = ai_service
        self.db_session_factory = db_session_factory
        self._ai_blocked_until_by_scope: dict[str, float] = {
            "default": 0.0,
            "reference": 0.0,
            "grounded_search": 0.0,
        }
        self._query_ai_blocked_until_by_scope: dict[tuple[str, str], float] = {}
        self._grounded_query_quota_cooldown_seconds = 180.0
        self._reference_profile_cache: dict[str, dict[str, Any] | None] = {}
        self._verified_external_url_cache: dict[str, str] = {}
        self._verified_external_source_url_cache: dict[str, str] = {}
        self._external_url_probe_cache: dict[str, dict[str, Any]] = {}
        logger.info("[AnalogService] Initialized.")

    @staticmethod
    def _extract_retry_after_seconds(error_text: str) -> float:
        match = re.search(
            r"retry(?:\s+in|\s*delay)?[^0-9]*([0-9]+(?:\.[0-9]+)?)s",
            str(error_text or "").lower(),
        )
        if not match:
            return 0.0
        try:
            return max(0.0, float(match.group(1)))
        except Exception:
            return 0.0

    @staticmethod
    def _normalize_ai_block_scope(scope: str | None) -> str:
        scope_value = str(scope or "").strip().lower()
        if scope_value in {"reference", "reference_lookup"}:
            return "reference"
        if scope_value in {"grounded", "search", "grounded_search", "internet_search"}:
            return "grounded_search"
        if scope_value in {"default", "validation"}:
            return "default"
        return "default"

    def _remaining_ai_block_seconds(self, scope: str | None = None) -> int:
        now = time.time()
        if scope is None:
            remaining = max(self._ai_blocked_until_by_scope.values(), default=0.0) - now
            return max(0, int(remaining))

        scope_value = str(scope or "").strip().lower()
        if scope_value == "grounded":
            remaining = max(
                self._ai_blocked_until_by_scope.get("reference", 0.0),
                self._ai_blocked_until_by_scope.get("grounded_search", 0.0),
            ) - now
            return max(0, int(remaining))

        normalized_scope = self._normalize_ai_block_scope(scope)
        remaining = self._ai_blocked_until_by_scope.get(normalized_scope, 0.0) - now
        return max(0, int(remaining))

    def _is_ai_temporarily_blocked(self, scope: str | None = None) -> bool:
        return self._remaining_ai_block_seconds(scope) > 0

    def _block_ai_temporarily(
        self,
        seconds: float = 10.0,
        reason: str = "quota exhausted",
        *,
        scope: str = "default",
    ) -> None:
        effective_seconds = max(10.0, float(seconds or 0.0))
        normalized_scope = self._normalize_ai_block_scope(scope)
        current_blocked_until = self._ai_blocked_until_by_scope.get(normalized_scope, 0.0)
        blocked_until = max(current_blocked_until, time.time() + effective_seconds)
        self._ai_blocked_until_by_scope[normalized_scope] = blocked_until
        logger.warning(
            f"[AnalogService] AI blocked for {int(effective_seconds)} seconds "
            f"(until {time.strftime('%H:%M:%S', time.localtime(blocked_until))}) "
            f"scope='{normalized_scope}' reason='{reason}'"
        )

    @staticmethod
    def _normalize_query_retry_text(value: str | None) -> str:
        normalized = re.sub(r"\s+", " ", str(value or "")).strip().lower()
        return normalized[:1000]

    def _build_query_retry_signature(self, query: str, requirements: str = "") -> str:
        clean_query = self._clean_search_query(query)
        normalized_query = self._normalize_query_retry_text(clean_query)
        normalized_requirements = self._normalize_query_retry_text(requirements)
        if normalized_requirements:
            return f"{normalized_query} || {normalized_requirements}"
        return normalized_query

    def _remaining_query_ai_block_seconds(
        self,
        query: str,
        requirements: str = "",
        *,
        scope: str = "grounded_search",
    ) -> int:
        normalized_scope = self._normalize_ai_block_scope(scope)
        signature = self._build_query_retry_signature(query, requirements)
        if not signature:
            return 0
        remaining = self._query_ai_blocked_until_by_scope.get((normalized_scope, signature), 0.0) - time.time()
        return max(0, int(remaining))

    def _is_query_ai_temporarily_blocked(
        self,
        query: str,
        requirements: str = "",
        *,
        scope: str = "grounded_search",
    ) -> bool:
        normalized_scope = self._normalize_ai_block_scope(scope)
        signature = self._build_query_retry_signature(query, requirements)
        if not signature:
            return False
        blocked_until = self._query_ai_blocked_until_by_scope.get((normalized_scope, signature), 0.0)
        return blocked_until > time.time()

    def _block_query_ai_temporarily(
        self,
        query: str,
        requirements: str = "",
        *,
        seconds: float = 180.0,
        reason: str = "quota exhausted",
        scope: str = "grounded_search",
    ) -> None:
        normalized_scope = self._normalize_ai_block_scope(scope)
        signature = self._build_query_retry_signature(query, requirements)
        if not signature:
            return
        effective_seconds = max(self._grounded_query_quota_cooldown_seconds, float(seconds or 0.0))
        current_blocked_until = self._query_ai_blocked_until_by_scope.get((normalized_scope, signature), 0.0)
        blocked_until = max(current_blocked_until, time.time() + effective_seconds)
        self._query_ai_blocked_until_by_scope[(normalized_scope, signature)] = blocked_until
        logger.warning(
            "[AnalogService] Query AI cooldown for %s seconds (until %s) scope='%s' reason='%s' query='%s'",
            int(effective_seconds),
            time.strftime('%H:%M:%S', time.localtime(blocked_until)),
            normalized_scope,
            reason,
            query[:120],
        )

    def _clear_query_ai_block(
        self,
        query: str,
        requirements: str = "",
        *,
        scope: str = "grounded_search",
    ) -> None:
        normalized_scope = self._normalize_ai_block_scope(scope)
        signature = self._build_query_retry_signature(query, requirements)
        if not signature:
            return
        self._query_ai_blocked_until_by_scope.pop((normalized_scope, signature), None)

    @staticmethod
    def _is_search_result_url(parsed_url) -> bool:
        host = str(parsed_url.netloc or "").lower()
        path = str(parsed_url.path or "").lower()

        if not host:
            return True

        if re.fullmatch(r"(?:www\.)?google\.[a-z.]+", host):
            return True
        if re.fullmatch(r"(?:www\.)?yandex\.[a-z.]+", host):
            return True
        if host in {
            "bing.com",
            "www.bing.com",
            "go.mail.ru",
            "search.yahoo.com",
            "search.yahoo.co.jp",
        }:
            return True
        if host.endswith("googleusercontent.com"):
            return True
        if path.startswith("/search") or path.startswith("/url"):
            return True

        return False

    def _normalize_external_url(self, raw_url: Any) -> str:
        raw = str(raw_url or "").replace("\\/", "/").strip()
        if not raw:
            return ""

        markdown_match = re.search(r"\((https?://[^)\s]+)\)", raw, flags=re.IGNORECASE)
        if markdown_match:
            raw = markdown_match.group(1)
        else:
            angle_match = re.search(r"<(https?://[^>\s]+)>", raw, flags=re.IGNORECASE)
            if angle_match:
                raw = angle_match.group(1)

        raw = raw.strip().strip("\"'`<>[]()")
        url_match = re.search(
            r"((?:https?://|www\.)[^\s<>'\"`]+|(?:[a-z0-9-]+\.)+[a-z]{2,}(?:/[^\s<>'\"`]+)?)",
            raw,
            flags=re.IGNORECASE,
        )
        if url_match:
            raw = url_match.group(1)

        raw = raw.rstrip(".,;:!?)]}")
        if not raw:
            return ""

        if not re.match(r"^[a-z][a-z0-9+.-]*://", raw, flags=re.IGNORECASE):
            if re.match(r"^(?:www\.)?(?:[a-z0-9-]+\.)+[a-z]{2,}(?:/|$)", raw, flags=re.IGNORECASE):
                raw = f"https://{raw.lstrip('/')}"
            else:
                return ""

        try:
            parsed = urlparse(raw)
        except Exception:
            return ""

        scheme = str(parsed.scheme or "").lower()
        host = str(parsed.netloc or "").strip().lower()
        if scheme not in {"http", "https"}:
            return ""
        if not host or " " in host or "." not in host:
            return ""

        try:
            host = host.encode("idna").decode("ascii")
        except Exception:
            pass

        path = re.sub(r"/{2,}", "/", parsed.path or "/")
        if host.endswith("gidroizol.ru"):
            path = path.rstrip("/") or "/"
            normalized = parsed._replace(
                scheme=scheme,
                netloc=host,
                path=path,
                params="",
                query="",
                fragment="",
            )
        else:
            normalized = parsed._replace(scheme=scheme, netloc=host, path=path)
        if self._is_search_result_url(normalized):
            return ""

        return urlunparse(normalized)

    def _build_external_url_validation_tokens(self, title: str, manufacturer: str = "") -> list[str]:
        tokens: list[str] = []
        seen: set[str] = set()
        combined = f"{manufacturer} {title}".strip()

        for token in sorted(self._extract_brand_tokens(combined)):
            normalized = self._normalize_text(token).strip()
            if normalized and normalized not in seen:
                seen.add(normalized)
                tokens.append(normalized)

        for token in sorted(self._extract_marks(title or "")):
            normalized = self._normalize_text(token).strip()
            if normalized and normalized not in seen:
                seen.add(normalized)
                tokens.append(normalized)

        for token in self._tokenize(combined):
            normalized = self._normalize_text(token).strip()
            if (
                normalized
                and normalized not in seen
                and self._is_reference_product_token(normalized)
            ):
                seen.add(normalized)
                tokens.append(normalized)

        return tokens[:6]

    @staticmethod
    def _looks_like_error_page(text: str) -> bool:
        raw_html = str(text or "")
        if not raw_html:
            return False

        cleaned = re.sub(
            r"<(script|style|svg|noscript)[^>]*>.*?</\1>",
            " ",
            raw_html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        cleaned = re.sub(r"<[^>]+>", " ", cleaned, flags=re.IGNORECASE)
        normalized = html.unescape(cleaned).lower()
        normalized = re.sub(r"\s+", " ", normalized).strip()
        if not normalized:
            return False
        phrase_markers = (
            "страница не найдена",
            "page not found",
            "not found",
            "ошибка 404",
            "error 404",
            "доступ запрещен",
            "access denied",
            "forbidden",
            "запрошенная страница не существует",
        )
        if any(marker in normalized for marker in phrase_markers):
            return True

        return bool(
            re.search(
                r"\b(?:error|ошибка)\s*404\b|\b404\s+(?:not found|страница не найдена)\b",
                normalized,
                flags=re.IGNORECASE,
            )
        )

    def _fetch_external_url_metadata(self, url: str) -> dict[str, Any]:
        normalized_url = self._normalize_external_url(url)
        if not normalized_url:
            return {"url": "", "error": "invalid_url"}

        cached = self._external_url_probe_cache.get(normalized_url)
        if cached is not None:
            return cached

        response = None
        metadata: dict[str, Any] = {
            "url": normalized_url,
            "final_url": normalized_url,
            "status_code": 0,
            "text": "",
            "content_type": "",
            "error": "",
        }
        try:
            last_error = None
            for timeout_seconds in (8, 15):
                try:
                    response = requests.get(
                        normalized_url,
                        timeout=timeout_seconds,
                        allow_redirects=True,
                        headers={
                            "User-Agent": (
                                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) "
                                "Chrome/124.0 Safari/537.36"
                            ),
                            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
                        },
                    )
                    break
                except Exception as exc:
                    last_error = exc
                    response = None
            if response is None:
                raise last_error or RuntimeError("unknown external URL validation error")

            metadata["final_url"] = self._normalize_external_url(response.url or normalized_url) or normalized_url
            metadata["status_code"] = int(getattr(response, "status_code", 0) or 0)
            metadata["content_type"] = str(response.headers.get("Content-Type", "")).lower()
            try:
                metadata["text"] = str(getattr(response, "text", "") or "")[:50000]
            except Exception:
                metadata["text"] = ""
        except Exception as exc:
            metadata["error"] = str(exc)
        finally:
            try:
                if response is not None:
                    response.close()
            except Exception:
                pass

        self._external_url_probe_cache[normalized_url] = metadata
        return metadata

    def _validate_external_source_url(
        self,
        url: str,
        *,
        title: str = "",
        manufacturer: str = "",
    ) -> str:
        normalized_url = self._normalize_external_url(url)
        if not normalized_url:
            return ""

        cache_key = f"{normalized_url}|{self._normalize_text(manufacturer)}|{self._normalize_text(title)}"
        cached = self._verified_external_source_url_cache.get(cache_key)
        if cached is not None:
            return cached

        metadata = self._fetch_external_url_metadata(normalized_url)
        error = str(metadata.get("error") or "")
        if error:
            logger.warning(
                "[AnalogService] Rejected AI source URL due to request error for '%s': %s | url=%s",
                title,
                error,
                normalized_url,
            )
            self._verified_external_source_url_cache[cache_key] = ""
            return ""

        final_url = self._normalize_external_url(metadata.get("final_url") or normalized_url)
        if not final_url:
            self._verified_external_source_url_cache[cache_key] = ""
            return ""

        status_code = int(metadata.get("status_code") or 0)
        if status_code in {404, 410, 451}:
            logger.warning(
                "[AnalogService] Rejected AI source URL with dead status=%s for '%s': %s",
                status_code,
                title,
                final_url,
            )
            self._verified_external_source_url_cache[cache_key] = ""
            return ""

        if status_code >= 400 and status_code not in {401, 403, 405, 429}:
            logger.warning(
                "[AnalogService] Rejected AI source URL with bad status=%s for '%s': %s",
                status_code,
                title,
                final_url,
            )
            self._verified_external_source_url_cache[cache_key] = ""
            return ""

        page_text = str(metadata.get("text") or "")
        if self._looks_like_error_page(page_text):
            logger.warning(
                "[AnalogService] Rejected AI source URL due to error page content for '%s': %s",
                title,
                final_url,
            )
            self._verified_external_source_url_cache[cache_key] = ""
            return ""

        validation_tokens = self._build_external_url_validation_tokens(title, manufacturer)
        if validation_tokens:
            haystack = self._normalize_text(f"{final_url} {page_text}")
            if not any(
                any(variant in haystack for variant in self._identity_token_variants(token))
                for token in validation_tokens
            ):
                logger.warning(
                    "[AnalogService] Rejected AI source URL due to weak source-page match for '%s': %s",
                    title,
                    final_url,
                )
                self._verified_external_source_url_cache[cache_key] = ""
                return ""

        self._verified_external_source_url_cache[cache_key] = final_url
        return final_url

    def _validate_external_product_url(
        self,
        url: str,
        *,
        title: str = "",
        manufacturer: str = "",
    ) -> str:
        normalized_url = self._normalize_external_url(url)
        if not normalized_url:
            return ""

        cache_key = f"{normalized_url}|{self._normalize_text(manufacturer)}|{self._normalize_text(title)}"
        cached = self._verified_external_url_cache.get(cache_key)
        if cached is not None:
            return cached

        metadata = self._fetch_external_url_metadata(normalized_url)
        error = str(metadata.get("error") or "")
        if error:
            logger.warning(
                "[AnalogService] Rejected AI product URL due to request error for '%s': %s | url=%s",
                title,
                error,
                normalized_url,
            )
            self._verified_external_url_cache[cache_key] = ""
            return ""

        final_url = self._normalize_external_url(metadata.get("final_url") or normalized_url)
        if not final_url:
            logger.warning(
                "[AnalogService] Rejected AI product URL after redirect normalization for '%s': %s",
                title,
                normalized_url,
            )
            self._verified_external_url_cache[cache_key] = ""
            return ""

        status_code = int(metadata.get("status_code") or 0)
        if status_code in {404, 410, 451}:
            logger.warning(
                "[AnalogService] Rejected AI product URL with dead status=%s for '%s': %s",
                status_code,
                title,
                final_url,
            )
            self._verified_external_url_cache[cache_key] = ""
            return ""

        validation_tokens = self._build_external_url_validation_tokens(title, manufacturer)
        token_haystack = self._normalize_text(final_url)

        if status_code in {401, 403, 405, 429}:
            if validation_tokens and any(
                any(variant in token_haystack for variant in self._identity_token_variants(token))
                for token in validation_tokens
            ):
                self._verified_external_url_cache[cache_key] = final_url
                return final_url
            logger.warning(
                "[AnalogService] Rejected AI product URL with restricted status=%s for '%s': %s",
                status_code,
                title,
                final_url,
            )
            self._verified_external_url_cache[cache_key] = ""
            return ""

        if status_code >= 400:
            logger.warning(
                "[AnalogService] Rejected AI product URL with bad status=%s for '%s': %s",
                status_code,
                title,
                final_url,
            )
            self._verified_external_url_cache[cache_key] = ""
            return ""

        content_type = str(metadata.get("content_type") or "").lower()
        if content_type and "html" not in content_type and "xhtml" not in content_type:
            logger.warning(
                "[AnalogService] Rejected AI product URL with non-HTML content-type '%s' for '%s': %s",
                content_type,
                title,
                final_url,
            )
            self._verified_external_url_cache[cache_key] = ""
            return ""

        page_text = str(metadata.get("text") or "")

        if self._looks_like_error_page(page_text):
            logger.warning(
                "[AnalogService] Rejected AI product URL due to error page content for '%s': %s",
                title,
                final_url,
            )
            self._verified_external_url_cache[cache_key] = ""
            return ""

        if validation_tokens:
            haystack = self._normalize_text(f"{final_url} {page_text}")
            matched_tokens = 0
            for token in validation_tokens:
                if any(variant in haystack for variant in self._identity_token_variants(token)):
                    matched_tokens += 1
            required_matches = 1 if len(validation_tokens) <= 2 else 2
            if matched_tokens < required_matches:
                logger.warning(
                    "[AnalogService] Rejected AI product URL due to weak product-page match for '%s': %s",
                    title,
                    final_url,
                )
                self._verified_external_url_cache[cache_key] = ""
                return ""

        self._verified_external_url_cache[cache_key] = final_url
        return final_url

    @staticmethod
    def _strip_html_tags(value: str) -> str:
        cleaned = re.sub(r"<[^>]+>", " ", str(value or ""), flags=re.IGNORECASE)
        cleaned = html.unescape(cleaned)
        return re.sub(r"\s+", " ", cleaned).strip()

    def _extract_html_candidate_links(self, page_text: str, base_url: str) -> list[dict[str, str]]:
        html_text = str(page_text or "")
        if not html_text:
            return []

        base_host = str(urlparse(base_url).netloc or "").lower()
        candidates: list[dict[str, str]] = []
        seen_urls: set[str] = set()

        def add_candidate(raw_link: str, label: str = "") -> None:
            normalized_url = self._normalize_external_url(urljoin(base_url, str(raw_link or "").strip()))
            if not normalized_url:
                return
            parsed = urlparse(normalized_url)
            candidate_host = str(parsed.netloc or "").lower()
            if base_host and candidate_host and candidate_host != base_host:
                return
            if normalized_url in seen_urls:
                return
            if parsed.fragment:
                return
            if self._is_search_result_url(parsed):
                return
            seen_urls.add(normalized_url)
            candidates.append(
                {
                    "url": normalized_url,
                    "label": self._strip_html_tags(label),
                }
            )

        for match in re.finditer(
            r"<link[^>]+rel=[\"'][^\"']*canonical[^\"']*[\"'][^>]+href=[\"']([^\"']+)[\"']",
            html_text,
            flags=re.IGNORECASE,
        ):
            add_candidate(match.group(1), "canonical")

        for match in re.finditer(
            r"<meta[^>]+property=[\"']og:url[\"'][^>]+content=[\"']([^\"']+)[\"']",
            html_text,
            flags=re.IGNORECASE,
        ):
            add_candidate(match.group(1), "og:url")

        for match in re.finditer(
            r"<a[^>]+href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>",
            html_text,
            flags=re.IGNORECASE | re.DOTALL,
        ):
            add_candidate(match.group(1), match.group(2))

        return candidates

    def _resolve_product_url_from_source_page(
        self,
        *,
        source_url: str,
        candidate_title: str,
        candidate_manufacturer: str = "",
        used_urls: Optional[set[str]] = None,
    ) -> str:
        validated_source_url = self._validate_external_source_url(
            source_url,
            title=candidate_title,
            manufacturer=candidate_manufacturer,
        )
        if not validated_source_url:
            return ""

        metadata = self._fetch_external_url_metadata(validated_source_url)
        if metadata.get("error"):
            return ""

        identity_tokens = self._build_external_url_validation_tokens(
            candidate_title,
            candidate_manufacturer,
        )
        links = self._extract_html_candidate_links(
            str(metadata.get("text") or ""),
            validated_source_url,
        )
        ranked_links: list[dict[str, Any]] = []
        for link in links:
            link_url = str(link.get("url") or "")
            if not link_url or (used_urls and link_url in used_urls):
                continue
            label = str(link.get("label") or "")
            haystack = self._normalize_text(f"{link_url} {label}")
            token_matches = 0
            for token in identity_tokens:
                if any(variant in haystack for variant in self._identity_token_variants(token)):
                    token_matches += 1
            if token_matches <= 0:
                continue
            product_bonus = 0
            if re.search(r"/(product|catalog|item|goods|shop|market|prod|p)/", link_url, re.IGNORECASE):
                product_bonus += 2
            if label:
                product_bonus += 1
            ranked_links.append(
                {
                    "url": link_url,
                    "score": token_matches * 10 + product_bonus,
                }
            )

        ranked_links.sort(key=lambda item: int(item.get("score") or 0), reverse=True)
        for link in ranked_links[:10]:
            validated_link = self._validate_external_product_url(
                link.get("url", ""),
                title=candidate_title,
                manufacturer=candidate_manufacturer,
            )
            if validated_link:
                logger.info(
                    "[AnalogService] Resolved product URL from source page for '%s': %s",
                    candidate_title,
                    validated_link,
                )
                return validated_link

        direct_from_source = self._validate_external_product_url(
            validated_source_url,
            title=candidate_title,
            manufacturer=candidate_manufacturer,
        )
        if direct_from_source:
            return direct_from_source

        return ""

    def _select_confirmed_source(
        self,
        *,
        candidate_title: str,
        candidate_manufacturer: str = "",
        grounding_sources: Any = None,
        preferred_source_title: str = "",
        preferred_source_url: str = "",
    ) -> dict[str, str]:
        ranked_sources = self._rank_grounding_sources(
            candidate_title=candidate_title,
            candidate_manufacturer=candidate_manufacturer,
            grounding_sources=grounding_sources,
            preferred_source_title=preferred_source_title,
            preferred_source_url=preferred_source_url,
        )

        source_candidates: list[dict[str, str]] = []
        if preferred_source_url:
            source_candidates.append(
                {
                    "title": preferred_source_title or candidate_title,
                    "url": preferred_source_url,
                }
            )
        source_candidates.extend(
            {"title": str(source.get("title") or ""), "url": str(source.get("url") or "")}
            for source in ranked_sources
        )

        seen_urls: set[str] = set()
        fallback_title = ""
        for source in source_candidates:
            raw_url = str(source.get("url") or "")
            normalized_raw_url = self._normalize_external_url(raw_url)
            if not normalized_raw_url or normalized_raw_url in seen_urls:
                continue
            seen_urls.add(normalized_raw_url)
            if not fallback_title:
                fallback_title = str(source.get("title") or "")
            validated_source_url = self._validate_external_source_url(
                normalized_raw_url,
                title=candidate_title,
                manufacturer=candidate_manufacturer,
            )
            if validated_source_url:
                return {
                    "title": str(source.get("title") or ""),
                    "url": validated_source_url,
                }

        if fallback_title:
            return {"title": fallback_title, "url": ""}
        return {}

    def _rank_grounding_sources(
        self,
        *,
        candidate_title: str,
        candidate_manufacturer: str = "",
        grounding_sources: Any = None,
        preferred_source_title: str = "",
        preferred_source_url: str = "",
    ) -> list[dict[str, Any]]:
        sources = grounding_sources or []
        if not isinstance(sources, list):
            return []

        identity_tokens = self._build_external_url_validation_tokens(
            candidate_title or "",
            candidate_manufacturer or "",
        )
        title_tokens = [
            token
            for token in self._tokenize(f"{candidate_manufacturer} {candidate_title}")
            if not self._is_noise_token(token)
        ]
        preferred_title_tokens = [
            token
            for token in self._tokenize(preferred_source_title)
            if not self._is_noise_token(token)
        ]
        candidate_tokens = []
        seen_tokens: set[str] = set()
        for token in identity_tokens + title_tokens + preferred_title_tokens:
            normalized = self._normalize_text(token).strip()
            if normalized and normalized not in seen_tokens:
                seen_tokens.add(normalized)
                candidate_tokens.append(normalized)
        if not candidate_tokens:
            return []

        preferred_source_url_normalized = self._normalize_external_url(preferred_source_url)
        ranked_sources: list[dict[str, Any]] = []

        for source in sources:
            if not isinstance(source, dict):
                continue
            raw_url = str(source.get("url") or "").strip()
            raw_title = str(source.get("title") or "").strip()
            normalized_url = self._normalize_external_url(raw_url)
            if not normalized_url:
                continue

            haystack = self._normalize_text(f"{raw_title} {normalized_url}")
            identity_matches = 0
            generic_matches = 0
            preferred_title_matches = 0
            for token in candidate_tokens:
                variants = self._identity_token_variants(token)
                if any(variant in haystack for variant in variants):
                    if token in identity_tokens:
                        identity_matches += 1
                    elif token in preferred_title_tokens:
                        preferred_title_matches += 1
                    else:
                        generic_matches += 1

            if identity_matches <= 0 and generic_matches < 2 and preferred_title_matches <= 0:
                continue

            title_bonus = 1 if raw_title else 0
            product_path_bonus = 1 if re.search(r"/(product|catalog|item|goods|shop|market|prod|p)/", normalized_url, re.IGNORECASE) else 0
            preferred_url_bonus = 50 if preferred_source_url_normalized and normalized_url == preferred_source_url_normalized else 0
            score = (
                identity_matches * 10
                + generic_matches * 2
                + preferred_title_matches * 5
                + title_bonus
                + product_path_bonus
                + preferred_url_bonus
            )
            ranked_sources.append(
                {
                    "score": score,
                    "title": raw_title,
                    "url": normalized_url,
                }
            )

        ranked_sources.sort(key=lambda item: int(item.get("score") or 0), reverse=True)
        return ranked_sources

    def _select_grounding_source_url(
        self,
        *,
        candidate_title: str,
        candidate_manufacturer: str = "",
        grounding_sources: Any = None,
        preferred_source_title: str = "",
        preferred_source_url: str = "",
        used_urls: Optional[set[str]] = None,
    ) -> str:
        ranked_sources = self._rank_grounding_sources(
            candidate_title=candidate_title,
            candidate_manufacturer=candidate_manufacturer,
            grounding_sources=grounding_sources,
            preferred_source_title=preferred_source_title,
            preferred_source_url=preferred_source_url,
        )

        for source in ranked_sources:
            raw_title = str(source.get("title") or "")
            normalized_url = str(source.get("url") or "")
            if used_urls and normalized_url in used_urls:
                continue
            validated_url = self._validate_external_product_url(
                normalized_url,
                title=candidate_title,
                manufacturer=candidate_manufacturer,
            )
            if validated_url:
                logger.info(
                    "[AnalogService] Selected grounding source URL for '%s': %s | source_title=%s",
                    candidate_title,
                    validated_url,
                    raw_title,
                )
                return validated_url

        return ""

    def _normalize_text(self, text: str) -> str:
        text = (text or "").lower().replace("\xa0", " ")
        text = re.sub(r"[^a-zа-я0-9.,+\-]+", " ", text, flags=re.IGNORECASE)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _extract_family_numeric_markers(self, text: str) -> list[str]:
        normalized = self._normalize_text(text or "")
        if not normalized:
            return []

        families = self._extract_material_families(normalized)
        markers: list[str] = []

        def add_marker(value: str) -> None:
            cleaned = self._normalize_text(value).strip()
            if cleaned and cleaned not in markers:
                markers.append(cleaned)

        if "bitumen" in families:
            for left, _right in re.findall(r"\b(\d{2,3})\s*(?:/|\s)\s*(\d{1,2})\b", normalized):
                add_marker(left)
            weight_match = re.search(r"\b(\d{2,3})\s*кг\b", normalized)
            if weight_match:
                add_marker(f"{weight_match.group(1)}кг")

        if {"geotextile", "geomembrane", "shponka"} & families:
            for raw_value in re.findall(r"\b(\d{2,4})\b", normalized):
                try:
                    numeric_value = int(raw_value)
                except Exception:
                    continue
                if 50 <= numeric_value <= 1200:
                    add_marker(raw_value)

        return markers[:5]

    def _sanitize_fts_term(self, term: str) -> list[str]:
        normalized = self._normalize_text(term or "").replace(",", " ")
        normalized = re.sub(r"[\-+/]+", " ", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        if not normalized:
            return []

        tokens: list[str] = []
        family_numeric_markers = set(self._extract_family_numeric_markers(term))
        for token in normalized.split():
            token = token.strip()
            if len(token) < 2:
                continue
            if token not in family_numeric_markers and self._is_noise_token(token, allow_dimensions=True):
                continue
            if token not in tokens:
                tokens.append(token)
        for marker in family_numeric_markers:
            if marker not in tokens:
                tokens.append(marker)
        return tokens

    def _build_fts_match_expression(self, terms: Sequence[str]) -> str:
        clauses: list[str] = []
        seen: set[str] = set()
        for term in terms:
            for token in self._sanitize_fts_term(term):
                if token in seen:
                    continue
                seen.add(token)
                clauses.append(f"{token}*")
                if len(clauses) >= 12:
                    break
            if len(clauses) >= 12:
                break
        return " OR ".join(clauses)

    def _load_candidate_ids_from_fts(
        self,
        session: Session,
        terms: Sequence[str],
        *,
        limit: int = 800,
    ) -> list[int]:
        from sqlalchemy import text

        match_expression = self._build_fts_match_expression(terms)
        if not match_expression:
            return []

        try:
            rows = session.execute(
                text(
                    """
                    SELECT product_id
                    FROM product_search_fts
                    WHERE product_search_fts MATCH :match
                    LIMIT :lim
                    """
                ),
                {"match": match_expression, "lim": int(limit)},
            ).fetchall()
        except Exception as e:
            logger.debug(f"[AnalogService] FTS lookup skipped: {e}")
            return []

        candidate_ids: list[int] = []
        for row in rows:
            product_id = getattr(row, "product_id", None)
            if product_id is None and row:
                product_id = row[0]
            try:
                candidate_ids.append(int(product_id))
            except Exception:
                continue
        return candidate_ids

    def _load_local_search_rows(
        self,
        session: Session,
        *,
        limit: int,
        candidate_ids: Optional[Sequence[int]] = None,
    ):
        from sqlalchemy import text

        base_sql = """
            SELECT
                p.id,
                p.title,
                p.category,
                p.material_type,
                p.price,
                p.specs,
                p.url,
                p.description,
                p.quality_score,
                p.normalized_category,
                ai.normalized_title,
                ai.material_group,
                ai.material_subgroup,
                ai.product_family,
                ai.base_material,
                ai.search_text
            FROM products p
            LEFT JOIN product_analog_index ai ON ai.product_id = p.id
            WHERE p.searchable_for_analogs = 1
        """
        params: dict[str, Any] = {"lim": int(limit)}

        if candidate_ids:
            placeholders = []
            for index, product_id in enumerate(candidate_ids):
                key = f"id_{index}"
                placeholders.append(f":{key}")
                params[key] = int(product_id)
            base_sql += f" AND p.id IN ({', '.join(placeholders)})"

        base_sql += " ORDER BY p.quality_score DESC, p.updated_at DESC LIMIT :lim"
        return session.execute(text(base_sql), params).fetchall()

    def _is_noise_token(self, token: str, *, allow_dimensions: bool = False) -> bool:
        token = (token or "").strip(" \t\r\n.,").lower()
        if not token:
            return True

        stopwords = {
            "и", "или", "для", "по", "на", "с", "из", "под", "над", "в",
            "во", "к", "ко", "от", "до", "не", "без", "при", "как",
            "быть", "должен", "должна", "должны", "только", "же",
            "материал", "товар", "требуется", "аналог", "серый", "черный",
            "поставка", "закупка", "техническое", "задание", "описание",
            "объекта", "документация", "характеристики", "требования",
            "менее", "более", "ниже", "выше", "стандарт", "гост", "ту", "сто",
            "бренд", "производитель", "модель", "тип", "вид", "класс",
            "линейка", "назначение", "размер", "рамках", "гоз", "доп",
            "мм", "см", "м", "м2", "м²", "м3", "м³", "кг", "г", "л",
            "шт", "штука", "штук", "ед", "единица", "рул", "рулон",
            "упак", "тара",
        }
        if token in stopwords:
            return True

        normalized = token.replace(",", ".")
        if re.fullmatch(r"[+-]?\d+(?:\.\d+)?", normalized):
            return True
        if re.fullmatch(r"[+-]?\d+(?:\.\d+)?(?:мм|см|м|кг|г|л|м2|м²|м3|м³|%)", normalized):
            return True
        if not allow_dimensions and re.fullmatch(r"\d+(?:[xх*]\d+)+(?:мм|см|м)?", normalized):
            return True
        if len(token) < 2:
            return True

        return False

    def _extract_query_descriptors(self, text: str) -> list[str]:
        generic_descriptors = {
            "битумный", "битумно", "битумно-полимерный", "полимерный",
            "полимерная", "гидроизоляционный", "гидроизоляционная",
            "кровельный", "кровельная", "рулонный", "рулонная",
            "профилированная", "наплавляемый", "нижний", "верхний",
            "слой", "материал", "мембрана", "мастика", "праймер",
            "герметик", "основа", "стеклоткань", "пленка", "сланец",
            "геотекстиль", "геомембрана", "битум", "гидрошпонка",
        }

        descriptors: list[str] = []
        for token in self._tokenize(text):
            if token in generic_descriptors:
                continue
            if re.search(r"[a-z0-9]", token, flags=re.IGNORECASE):
                continue
            if len(token) < 6:
                continue
            if token not in descriptors:
                descriptors.append(token)

        return descriptors

    @staticmethod
    def _generic_identity_stopwords() -> set[str]:
        return {
            "материал", "материалы", "товар", "аналог", "тип", "вид",
            "праймер", "мастика", "герметик", "мембрана", "пленка",
            "лента", "шпонка", "гидрошпонка", "утеплитель", "профнастил", "конек",
            "планка", "уголок", "стеклопластик", "рулонный", "рулонная",
            "рулонное", "кровельный", "кровельная", "гидроизоляционный",
            "гидроизоляционная", "битумный", "битумно", "полимерный",
            "полимерная", "профилированная", "профилированный",
            "наплавляемый", "наплавляемая", "нижний", "верхний",
            "оцинкованный", "оцинкованная", "металлический", "металлическая",
            "внутренний", "наружный", "внешний", "основа", "слой", "слоя",
            "слоев", "стеклоткань", "толщина", "масса", "гибкость",
            "теплостойкость", "водопоглощение", "прочность", "сцепление",
            "вязкость", "размер", "длина", "ширина", "температура",
            "радиус", "давление", "время", "содержание", "показатель",
            "наличие", "прочность",
            "фасовка", "фасовке", "фасовку", "фасовки",
            "упаковка", "упаковке", "упаковку", "упаковки",
            "ведро", "ведра", "ведре", "ведром", "ведер",
            "изоляционный", "изоляционная", "изоляционное", "изоляционные",
            "изоляция",
            "полиэфирный", "полиэфирная", "иглопробивной", "иглопробивная",
            "фильерный", "фильерная",
            "минеральная", "минеральный", "минеральное", "минеральные",
            "минвата", "стекловата",
            "фольга", "фольгированная", "фольгированный",
            "фольгированное", "фольгированные",
            "металлизированная", "металлизированный",
            "резина", "резиновый", "резиновая", "резиновое", "резиновые",
            "посыпка", "посыпкой", "посыпке", "посыпку", "посыпки",
            "стеклохолст", "полиэстер", "полиэтилен", "сланец", "пленка",
            "геотекстиль", "геомембрана", "битум", "брикет",
            "серый", "черный", "белый", "красный", "синий", "зеленый",
            "коричневый", "шоколад", "темный", "светлый", "сигнальный",
            "сигнально", "стандарт", "бизнес", "премиум", "эконом",
            "проф", "профи", "готовый", "концентрат", "цвет", "ral",
            "пвх", "pvc", "tpo", "epdm", "xps", "geo",
        }

    @staticmethod
    def _generic_identity_prefixes() -> tuple[str, ...]:
        return (
            "битум",
            "гидроизоляц",
            "геотекст",
            "геомембран",
            "гидрошпонк",
            "кровельн",
            "рулонн",
            "полимерн",
            "профилирован",
            "наплавля",
            "оцинкован",
            "металлическ",
            "армирован",
            "самокле",
            "холодн",
            "горяч",
            "водоэмульс",
            "стеклоткан",
            "стеклохолст",
            "толщин",
            "масс",
            "гибк",
            "теплостойк",
            "водопоглощ",
            "прочност",
            "сцеплен",
            "вязк",
            "размер",
            "длин",
            "ширин",
            "температур",
            "радиус",
            "давлен",
            "врем",
            "содержан",
            "показател",
            "налич",
            "изоляцион",
            "изоляц",
            "минерал",
            "фольг",
            "металлиз",
            "фасов",
            "упаков",
            "ведр",
            "резин",
            "полиэстер",
            "полиэтилен",
            "полиэфир",
            "иглопробив",
            "фильер",
            "посып",
            "крупнозернист",
            "мелкозернист",
            "крупно",
            "мелко",
        )

    @staticmethod
    def _transliterate_latin_to_cyrillic(token: str) -> str:
        value = str(token or "").lower()
        if not value or not re.fullmatch(r"[a-z0-9-]+", value):
            return ""

        multi_map = [
            ("shch", "щ"),
            ("sch", "щ"),
            ("yo", "ё"),
            ("zh", "ж"),
            ("kh", "х"),
            ("ts", "ц"),
            ("ch", "ч"),
            ("sh", "ш"),
            ("yu", "ю"),
            ("ya", "я"),
            ("ye", "е"),
            ("jo", "ё"),
            ("ju", "ю"),
            ("ja", "я"),
        ]
        single_map = {
            "a": "а", "b": "б", "c": "к", "d": "д", "e": "е", "f": "ф",
            "g": "г", "h": "х", "i": "и", "j": "й", "k": "к", "l": "л",
            "m": "м", "n": "н", "o": "о", "p": "п", "q": "к", "r": "р",
            "s": "с", "t": "т", "u": "у", "v": "в", "w": "в", "x": "кс",
            "y": "и", "z": "з",
        }

        result = []
        index = 0
        while index < len(value):
            matched = False
            for latin, cyr in multi_map:
                if value.startswith(latin, index):
                    result.append(cyr)
                    index += len(latin)
                    matched = True
                    break
            if matched:
                continue
            char = value[index]
            result.append(single_map.get(char, char))
            index += 1
        return "".join(result)

    @staticmethod
    def _transliterate_cyrillic_to_latin(token: str) -> str:
        value = str(token or "").lower()
        if not value or not re.fullmatch(r"[а-яё0-9-]+", value):
            return ""

        char_map = {
            "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e",
            "ё": "yo", "ж": "zh", "з": "z", "и": "i", "й": "y", "к": "k",
            "л": "l", "м": "m", "н": "n", "о": "o", "п": "p", "р": "r",
            "с": "s", "т": "t", "у": "u", "ф": "f", "х": "kh", "ц": "ts",
            "ч": "ch", "ш": "sh", "щ": "shch", "ы": "y", "э": "e",
            "ю": "yu", "я": "ya", "ь": "", "ъ": "",
        }
        return "".join(char_map.get(char, char) for char in value)

    def _identity_token_variants(self, token: str) -> set[str]:
        normalized = self._normalize_text(token).strip()
        if not normalized:
            return set()
        variants = {normalized}
        latin_variant = self._transliterate_cyrillic_to_latin(normalized)
        cyrillic_variant = self._transliterate_latin_to_cyrillic(normalized)
        if latin_variant:
            variants.add(latin_variant)
        if cyrillic_variant:
            variants.add(cyrillic_variant)
        return {variant for variant in variants if variant}

    def _is_generic_identity_token(self, token: str) -> bool:
        normalized = self._normalize_text(token).strip()
        if not normalized:
            return True
        if self._is_noise_token(normalized, allow_dimensions=True):
            return True
        if normalized in self._generic_identity_stopwords():
            return True
        if any(normalized.startswith(prefix) for prefix in self._generic_identity_prefixes()):
            return True
        if normalized in self._extract_marks(normalized):
            return True
        if re.fullmatch(r"(?:ral|rr)\d{3,4}", normalized, flags=re.IGNORECASE):
            return True
        if re.fullmatch(r"\d+(?:[xх*]\d+)+(?:мм|см|м)?", normalized):
            return True
        if re.fullmatch(r"[a-zа-я]*\d+[a-zа-я0-9-]*", normalized, flags=re.IGNORECASE):
            return True
        letters_only = re.sub(r"[^a-zа-я]+", "", normalized, flags=re.IGNORECASE)
        if len(letters_only) < 4:
            return True
        return False

    def _is_reference_product_token(self, token: str) -> bool:
        token = (token or "").strip(" \t\r\n.,").lower()
        if not token:
            return False
        if token in {"pvc", "tpo", "epdm", "xps", "тпп", "ткп", "хпп", "хкп", "эпп", "экп", "эмп"}:
            return False
        if token.startswith("№") or re.search(r"\d", token):
            return True
        return bool(self._extract_brand_tokens(token))

    def _extract_grade_tokens(self, text: str) -> set[str]:
        normalized = self._normalize_text(text)
        if not normalized:
            return set()
        return set(
            re.findall(
                r"\b(премиум|бизнес|стандарт|проф|профи|эконом)\b",
                normalized,
                flags=re.IGNORECASE,
            )
        )

    def _extract_brand_tokens(self, text: str) -> set[str]:
        normalized = self._normalize_text(text)
        if not normalized:
            return set()

        brands = set()
        for raw_token in normalized.split():
            parts = [part for part in re.split(r"[-/]", raw_token) if part]
            for part in parts:
                if self._is_generic_identity_token(part):
                    continue
                brands.update(self._identity_token_variants(part))
        return brands

    def _manufacturer_identity_tokens(self, text: str) -> list[str]:
        normalized = self._normalize_text(text)
        if not normalized:
            return []

        ignore = {
            "ооо", "зао", "оао", "пао", "ао", "ип", "мпк", "завод",
            "производство", "производитель", "торговый", "дом", "группа",
            "компания", "материалов", "материалы", "строительные", "системы",
            "кровельных", "кровельные", "систем", "система",
        }

        tokens: list[str] = []
        for raw_token in re.split(r"[\s\-]+", normalized):
            token = raw_token.strip(".,")
            if len(token) < 3:
                continue
            if token in ignore:
                continue
            if token not in tokens:
                tokens.append(token)
        return tokens

    def _manufacturer_matches(self, left: str, right: str) -> bool:
        left_tokens = self._manufacturer_identity_tokens(left)
        right_tokens = self._manufacturer_identity_tokens(right)
        if not left_tokens or not right_tokens:
            return False

        overlap = {
            token
            for token in left_tokens
            if token in right_tokens and len(token) >= 4
        }
        if overlap:
            return True

        left_joined = " ".join(left_tokens)
        right_joined = " ".join(right_tokens)
        return left_joined in right_joined or right_joined in left_joined

    def _build_scoring_tokens(self, query: str, requirements: str = "") -> list[str]:
        tokens: list[str] = []

        def add_token(token: str) -> None:
            normalized = (token or "").strip(" \t\r\n.,").lower()
            if not normalized:
                return
            if normalized not in tokens:
                tokens.append(normalized)

        for token in self._build_search_keywords(query, requirements):
            add_token(token)

        for token in self._extract_query_descriptors(query):
            if self._is_reference_product_token(token):
                continue
            add_token(token)

        family_tokens_map = {
            "primer": ["праймер"],
            "mastika": ["мастика"],
            "bitumen": ["битум", "брикет"],
            "ruberoid": ["рубероид"],
            "bitumen_roll": ["наплавляемый"],
            "pvc_membrane": ["пвх", "мембрана"],
            "tpo_membrane": ["tpo", "мембрана"],
            "epdm_membrane": ["epdm", "мембрана"],
            "membrane": ["мембрана"],
            "geomembrane": ["геомембрана", "профилированная", "дренажная"],
            "geotextile": ["геотекстиль", "полиэфирный", "иглопробивной"],
            "film": ["пленка"],
            "tape": ["лента"],
            "sealant": ["герметик"],
            "xps": ["xps", "пенополистирол"],
            "shponka": ["шпонка", "гидрошпонка", "гидропрокладка"],
            "profnastil": ["профнастил"],
        }
        base_tokens_map = {
            "polyester": ["полиэстер"],
            "fiberglass_tissue": ["стеклоткань"],
            "fiberglass_mat": ["стеклохолст"],
            "cardboard": ["картон"],
        }
        method_tokens_map = {
            "torch_applied": ["наплавляемый"],
            "self_adhesive": ["самоклеящийся"],
            "cold_applied": ["холодного", "нанесения"],
        }

        source = f"{query}\n{requirements}"
        for family in self._extract_material_families(source):
            for token in family_tokens_map.get(family, []):
                add_token(token)
        for mark in sorted(self._extract_marks(source)):
            add_token(mark)
        for base in self._extract_base_types(source):
            for token in base_tokens_map.get(base, []):
                add_token(token)
        for method in self._extract_application_methods(source):
            for token in method_tokens_map.get(method, []):
                add_token(token)
        for token in self._extract_family_numeric_markers(source):
            add_token(token)

        role = self._extract_roll_role(source)
        if role == "top":
            add_token("верхний")
        elif role == "underlay":
            add_token("подкладочный")

        if not tokens:
            for token in self._tokenize(query):
                add_token(token)

        return tokens

    @staticmethod
    def _normalize_match_score(score: Any) -> int:
        try:
            value = int(round(float(score)))
        except Exception:
            return 0
        return max(0, min(100, value))

    @staticmethod
    def _parse_price_value(raw_price: Any) -> Optional[float]:
        if raw_price is None or raw_price == "":
            return None
        if isinstance(raw_price, (int, float)):
            return float(raw_price)

        text = str(raw_price).strip().replace("\xa0", " ")
        text = re.sub(r"[^\d,.\-]+", "", text)
        if not text:
            return None
        if "," in text and "." in text:
            text = text.replace(" ", "").replace(",", "")
        else:
            text = text.replace(",", ".")
        try:
            value = float(text)
        except Exception:
            return None
        return value if value > 0 else None

    def _parse_first_json_object(self, response_text: str) -> dict[str, Any]:
        text = (response_text or "").replace("\ufeff", "").replace("\u200b", "").strip()
        if not text:
            raise json.JSONDecodeError("Empty JSON response", text, 0)

        decoder = json.JSONDecoder()
        last_error: Optional[json.JSONDecodeError] = None
        for idx, char in enumerate(text):
            if char != "{":
                continue
            try:
                parsed, _ = decoder.raw_decode(text[idx:])
            except json.JSONDecodeError as exc:
                last_error = exc
                continue
            if isinstance(parsed, dict):
                return parsed

        raise last_error or json.JSONDecodeError("No JSON object found", text, 0)

    def _build_reference_lookup_tokens(self, query: str) -> list[str]:
        raw_tokens: list[str] = []
        for token in self._tokenize(query):
            if token not in raw_tokens:
                raw_tokens.append(token)
            for part in [piece for piece in re.split(r"[-/]", token) if piece]:
                if len(part) < 2:
                    continue
                if part not in raw_tokens:
                    raw_tokens.append(part)
        normalized = self._normalize_text(query)
        for match in re.findall(r"\b\d+[a-zа-я]+\b", normalized, flags=re.IGNORECASE):
            if match not in raw_tokens:
                raw_tokens.append(match)
        for match in re.findall(r"№\s*\d+[a-zа-я-]*", query, flags=re.IGNORECASE):
            normalized_match = self._normalize_text(match).replace(" ", "")
            if normalized_match and normalized_match not in raw_tokens:
                raw_tokens.append(normalized_match)
            digits_only = re.sub(r"[^0-9a-zа-я]+", "", normalized_match, flags=re.IGNORECASE)
            if len(digits_only) >= 2 and digits_only not in raw_tokens:
                raw_tokens.append(digits_only)
        return raw_tokens[:12]

    def _extract_identifier_tokens(self, text: str) -> set[str]:
        normalized = self._normalize_text(text)
        if not normalized:
            return set()

        tokens = set()
        for token in self._tokenize(text):
            if re.search(r"\d", token):
                if not re.fullmatch(r"[+-]?\d+(?:\.\d+)?", token):
                    tokens.add(token)
                for part in [piece for piece in re.split(r"[-/]", token) if piece]:
                    if (
                        len(part) >= 2
                        and re.search(r"\d", part)
                        and not re.fullmatch(r"[+-]?\d+(?:\.\d+)?", part)
                    ):
                        tokens.add(part)

        for match in re.findall(r"\b[a-zа-я]{1,6}-\d+[a-zа-я-]*\b", normalized, flags=re.IGNORECASE):
            tokens.add(match)
        for match in re.findall(r"\b\d+[a-zа-я]+\b", normalized, flags=re.IGNORECASE):
            tokens.add(match)
        for match in re.findall(r"№\s*\d+[a-zа-я-]*", text, flags=re.IGNORECASE):
            normalized_match = self._normalize_text(match).replace(" ", "")
            if normalized_match:
                tokens.add(normalized_match)
            digits_only = re.sub(r"[^0-9a-zа-я]+", "", normalized_match, flags=re.IGNORECASE)
            if len(digits_only) >= 2:
                tokens.add(digits_only)

        return {token for token in tokens if len(token) >= 2}

    def _has_explicit_reference_identity(self, query: str) -> bool:
        normalized = self._normalize_text(query)
        tokens = self._tokenize(query)
        if self._extract_brand_tokens(query):
            return True
        if re.search(r"№\s*\d", query, flags=re.IGNORECASE):
            return True
        if re.search(
            r"\b(?!тпп|ткп|хпп|хкп|эпп|экп|эмп)[a-zа-я]{1,8}-\d+[a-zа-я-]*\b",
            normalized,
            flags=re.IGNORECASE,
        ):
            return True
        return any(
            re.search(r"[a-z]", token, flags=re.IGNORECASE) and not self._is_generic_identity_token(token)
            for token in tokens
        )

    def _has_reference_brand_identity(self, query: str) -> bool:
        return bool(self._extract_brand_tokens(query))

    def _score_reference_candidate(self, product: dict, query: str) -> int:
        title = self._normalize_text(product.get("title", ""))
        category = self._normalize_text(product.get("category", ""))
        material_type = self._normalize_text(product.get("material_type", ""))
        description = self._normalize_text(product.get("description", ""))
        specs_text = self._normalize_text(self._flatten_specs(product.get("specs") or {}))
        haystack = f"{title} {category} {material_type}".strip()
        full_haystack = f"{haystack} {description} {specs_text}".strip()

        normalized_query = self._normalize_text(query)
        query_tokens = self._build_reference_lookup_tokens(query)
        query_brands = self._extract_brand_tokens(query)
        query_grades = self._extract_grade_tokens(query)

        score = 0
        if normalized_query and normalized_query in title:
            score += 140

        title_hits = [token for token in query_tokens if token in title]
        full_hits = [token for token in query_tokens if token in haystack]
        if title_hits:
            score += min(90, len(title_hits) * 18)
        if full_hits:
            score += min(30, len([token for token in full_hits if token not in title_hits]) * 6)

        product_brands = self._extract_brand_tokens(haystack)
        if query_brands:
            if query_brands & product_brands:
                score += 80
            else:
                score -= 60

        product_grades = self._extract_grade_tokens(haystack)
        if query_grades and product_grades and query_grades & product_grades:
            score += 20

        query_marks = self._extract_marks(query)
        product_marks = self._extract_marks(haystack)
        if query_marks:
            if query_marks & product_marks:
                score += 45
            else:
                score -= 100

        query_identifiers = self._extract_identifier_tokens(query)
        product_identifiers = self._extract_identifier_tokens(haystack)
        if query_identifiers:
            if query_identifiers & product_identifiers:
                score += 60
            else:
                score -= 90

        query_families = self._extract_material_families(query)
        product_families = self._extract_material_families(haystack)
        if not product_families:
            product_families = self._extract_material_families(full_haystack)
        if query_families:
            if not product_families:
                return -100
            if self._families_are_compatible(query_families, product_families):
                score += 20
            else:
                return -100

        query_subtypes = self._extract_insulation_subtypes(query)
        if query_subtypes:
            product_subtypes = self._extract_insulation_subtypes(full_haystack)
            if not product_subtypes:
                return -100
            if self._insulation_subtypes_are_compatible(query_subtypes, product_subtypes):
                score += 30
            else:
                return -100

        query_surface_features = self._extract_surface_features(query)
        if query_surface_features:
            product_surface_features = self._extract_surface_features(full_haystack)
            if not product_surface_features:
                return -100
            if self._surface_features_are_compatible(query_surface_features, product_surface_features):
                score += 15
            else:
                return -100

        return score

    def _build_reference_profile_from_product(
        self,
        product: dict,
        *,
        source: str,
        strict_price: bool = False,
    ) -> dict[str, Any]:
        specs = product.get("specs") or {}
        price = self._parse_price_value(product.get("price"))
        product_payload = {
            "title": product.get("title", ""),
            "category": product.get("category", ""),
            "material_type": product.get("material_type", ""),
            "description": product.get("description", ""),
            "specs": specs,
        }
        identity_source = "\n".join([
            str(product.get("title") or ""),
            str(product.get("category") or ""),
            str(product.get("material_type") or ""),
            self._flatten_specs(specs),
        ])
        return {
            "title": product.get("title", ""),
            "manufacturer": self._extract_product_manufacturer(product),
            "price": price,
            "price_unit": product.get("price_unit", "") or "руб",
            "specs": specs,
            "metrics": self._extract_product_metrics(product_payload),
            "families": self._extract_material_families(identity_source),
            "marks": self._extract_marks(identity_source),
            "source": source,
            "strict_price": strict_price and price is not None,
        }

    @staticmethod
    def _format_numeric_value(value: Any) -> str:
        try:
            numeric = float(value)
        except Exception:
            text = str(value or "").strip()
            return text

        if numeric.is_integer():
            return str(int(numeric))
        return f"{numeric:.3f}".rstrip("0").rstrip(".")

    def _format_requirement_value(self, constraint: Optional[dict[str, Any]], unit: str) -> str:
        if not isinstance(constraint, dict):
            return ""

        value = self._format_numeric_value(constraint.get("value"))
        if not value:
            return ""

        mode = str(constraint.get("mode") or "").strip().lower()
        if mode == "min":
            return f"не менее {value} {unit}".strip()
        if mode == "max":
            return f"не более {value} {unit}".strip()
        return f"{value} {unit}".strip()

    def _extract_density_requirement_text(self, query: str, requirements: str = "") -> str:
        source = f"{query}\n{requirements}".replace(",", ".")
        patterns = [
            (r"(не\s+менее|не\s+ниже|от)\s*([+-]?\d+(?:\.\d+)?)\s*кг\s*/?\s*(?:м3|м³)", True),
            (r"(не\s+более|не\s+выше|до)\s*([+-]?\d+(?:\.\d+)?)\s*кг\s*/?\s*(?:м3|м³)", True),
            (r"([+-]?\d+(?:\.\d+)?)\s*кг\s*/?\s*(?:м3|м³)", False),
        ]
        for pattern, has_prefix in patterns:
            match = re.search(pattern, source, flags=re.IGNORECASE)
            if not match:
                continue
            if has_prefix:
                prefix = re.sub(r"\s+", " ", match.group(1).strip().lower())
                value = self._format_numeric_value(match.group(2))
                return f"{prefix} {value} кг/м3".strip()
            value = self._format_numeric_value(match.group(1))
            return f"{value} кг/м3".strip()
        return ""

    def _extract_temperature_range_text(self, query: str, requirements: str = "") -> str:
        source = f"{query}\n{requirements}".replace(",", ".")

        def format_signed(raw_value: str) -> str:
            raw_text = str(raw_value or "").strip()
            sign = ""
            if raw_text.startswith(("+", "-")):
                sign = raw_text[0]
                raw_text = raw_text[1:]
            formatted = self._format_numeric_value(raw_text)
            return f"{sign}{formatted}".strip()

        range_match = re.search(
            r"от\s*([+-]?\d+(?:\.\d+)?)\s*°?\s*[cс]\s*до\s*([+-]?\d+(?:\.\d+)?)\s*°?\s*[cс]",
            source,
            flags=re.IGNORECASE,
        )
        if range_match:
            low = format_signed(range_match.group(1))
            high = format_signed(range_match.group(2))
            return f"от {low} °C до {high} °C"

        max_match = re.search(
            r"до\s*([+-]?\d+(?:\.\d+)?)\s*°?\s*[cс]",
            source,
            flags=re.IGNORECASE,
        )
        min_match = re.search(
            r"от\s*([+-]?\d+(?:\.\d+)?)\s*°?\s*[cс]",
            source,
            flags=re.IGNORECASE,
        )
        if min_match and max_match:
            low = format_signed(min_match.group(1))
            high = format_signed(max_match.group(1))
            return f"от {low} °C до {high} °C"
        return ""

    def _build_query_reference_profile(
        self,
        query: str,
        requirements: str = "",
    ) -> Optional[dict[str, Any]]:
        clean_query = self._clean_search_query(query)
        if not clean_query:
            return None

        source = f"{clean_query}\n{requirements or ''}"
        specs: dict[str, str] = {}
        metrics: dict[str, Optional[float]] = {
            "thickness": None,
            "mass": None,
            "flex": None,
        }

        insulation_subtypes = self._extract_insulation_subtypes(source)
        insulation_labels = {
            "mineral_wool": "минеральная вата",
            "xps": "экструдированный пенополистирол",
            "pe_foam": "вспененный полиэтилен",
            "spray_foam": "напыляемый утеплитель",
        }
        if insulation_subtypes:
            specs["Подтип утеплителя"] = ", ".join(
                insulation_labels[subtype]
                for subtype in sorted(insulation_subtypes)
                if subtype in insulation_labels
            )

        surface_features = self._extract_surface_features(source)
        surface_labels = {
            "foil": "фольга",
            "metallized": "металлизированное покрытие",
        }
        if surface_features:
            specs["Облицовка"] = ", ".join(
                surface_labels[feature]
                for feature in sorted(surface_features)
                if feature in surface_labels
            )

        constraints = self._extract_requirement_constraints(clean_query, requirements or "")
        numeric_requirements = self._extract_requirements_numeric(clean_query, requirements or "")
        thickness_text = self._format_requirement_value(constraints.get("thickness"), "мм")
        if thickness_text:
            specs["Толщина"] = thickness_text
            metrics["thickness"] = float(constraints["thickness"]["value"])
        elif numeric_requirements.get("thickness") is not None:
            metrics["thickness"] = float(numeric_requirements["thickness"])
            specs["Толщина"] = f"{self._format_numeric_value(numeric_requirements['thickness'])} мм"

        density_text = self._extract_density_requirement_text(clean_query, requirements or "")
        if density_text:
            specs["Плотность"] = density_text

        temperature_text = self._extract_temperature_range_text(clean_query, requirements or "")
        if temperature_text:
            specs["Рабочая температура"] = temperature_text

        query_families = self._extract_material_families(source)
        marks = self._extract_marks(source)
        if not specs and not query_families and not marks:
            return None

        return {
            "title": clean_query.strip(),
            "manufacturer": "",
            "price": None,
            "price_unit": "руб",
            "specs": specs,
            "metrics": metrics,
            "families": query_families,
            "marks": marks,
            "source": "query_reference_profile",
            "strict_price": False,
        }

    def _build_query_profile_summary_text(self, query: str, requirements: str = "") -> str:
        profile = self._build_query_reference_profile(query, requirements)
        if not profile:
            return ""

        lines: list[str] = []
        brands = sorted(self._extract_brand_tokens(query))
        if brands:
            lines.append(f"Бренд/линейка исходного товара: {', '.join(brands[:3])}")

        identifiers = sorted(self._extract_identifier_tokens(query))
        if identifiers:
            lines.append(f"Идентификаторы исходного товара: {', '.join(identifiers[:4])}")

        for key, value in list((profile.get("specs") or {}).items())[:8]:
            lines.append(f"{key}: {value}")

        return "\n".join(lines[:10])

    def _reference_profile_quality(self, reference_profile: Optional[dict[str, Any]]) -> int:
        if not isinstance(reference_profile, dict) or not reference_profile:
            return 0

        specs = reference_profile.get("specs") or {}
        metrics = reference_profile.get("metrics") or {}
        quality = sum(1 for value in specs.values() if str(value or "").strip())
        quality += sum(2 for value in metrics.values() if value is not None)
        if reference_profile.get("manufacturer"):
            quality += 1
        if reference_profile.get("price") is not None:
            quality += 1
        return quality

    def _is_reference_profile_exact_match(
        self,
        reference_profile: Optional[dict[str, Any]],
        query: str,
    ) -> bool:
        if not isinstance(reference_profile, dict) or not reference_profile:
            return False

        reference_identity = "\n".join([
            str(reference_profile.get("title") or ""),
            str(reference_profile.get("manufacturer") or ""),
            self._flatten_specs(reference_profile.get("specs") or {}),
        ])
        query_brands = self._extract_brand_tokens(query)
        reference_brands = self._extract_brand_tokens(reference_identity)
        query_marks = self._extract_marks(query)
        reference_marks = self._extract_marks(reference_identity)
        query_families = self._extract_material_families(query)
        reference_families = reference_profile.get("families") or self._extract_material_families(reference_identity)

        brand_ok = not query_brands or bool(reference_brands and query_brands & reference_brands)
        marks_ok = not query_marks or bool(query_marks & reference_marks)
        families_ok = True
        if query_families and reference_families:
            families_ok = self._families_are_compatible(query_families, reference_families)
        return brand_ok and marks_ok and families_ok

    def _can_merge_reference_profiles(
        self,
        query: str,
        left: Optional[dict[str, Any]],
        right: Optional[dict[str, Any]],
    ) -> bool:
        if not left or not right:
            return False

        left_identity = "\n".join([
            str(left.get("title") or ""),
            str(left.get("manufacturer") or ""),
            self._flatten_specs(left.get("specs") or {}),
        ])
        right_identity = "\n".join([
            str(right.get("title") or ""),
            str(right.get("manufacturer") or ""),
            self._flatten_specs(right.get("specs") or {}),
        ])

        left_families = left.get("families") or self._extract_material_families(left_identity)
        right_families = right.get("families") or self._extract_material_families(right_identity)
        if left_families and right_families:
            if not self._families_are_compatible(left_families, right_families):
                return False

        query_brands = self._extract_brand_tokens(query)
        if query_brands:
            left_brands = self._extract_brand_tokens(left_identity)
            right_brands = self._extract_brand_tokens(right_identity)
            if left_brands and not (query_brands & left_brands):
                return False
            if right_brands and not (query_brands & right_brands):
                return False

        query_marks = self._extract_marks(query)
        if query_marks:
            left_marks = self._extract_marks(left_identity)
            right_marks = self._extract_marks(right_identity)
            if left_marks and not (query_marks & left_marks):
                return False
            if right_marks and not (query_marks & right_marks):
                return False

        return True

    def _merge_reference_profiles(
        self,
        primary: Optional[dict[str, Any]],
        secondary: Optional[dict[str, Any]],
    ) -> Optional[dict[str, Any]]:
        if not primary:
            return secondary
        if not secondary:
            return primary

        merged_specs = {
            **(secondary.get("specs") or {}),
            **(primary.get("specs") or {}),
        }
        merged_metrics = dict(secondary.get("metrics") or {})
        for key, value in (primary.get("metrics") or {}).items():
            if value is not None:
                merged_metrics[key] = value

        return {
            **secondary,
            **primary,
            "title": primary.get("title") or secondary.get("title") or "",
            "manufacturer": primary.get("manufacturer") or secondary.get("manufacturer") or "",
            "price": primary.get("price") if primary.get("price") is not None else secondary.get("price"),
            "price_unit": primary.get("price_unit") or secondary.get("price_unit") or "руб",
            "specs": merged_specs,
            "metrics": merged_metrics,
            "families": primary.get("families") or secondary.get("families") or set(),
            "marks": primary.get("marks") or secondary.get("marks") or set(),
            "strict_price": bool(primary.get("strict_price") or secondary.get("strict_price")),
        }

    def _score_reference_proxy_candidate(
        self,
        product: dict,
        query: str,
        *,
        require_brand_match: bool = True,
    ) -> int:
        product_payload = {
            "title": product.get("title", ""),
            "category": product.get("category", ""),
            "material_type": product.get("material_type", ""),
            "description": product.get("description", ""),
            "specs": product.get("specs") or {},
        }
        score = 0
        product_identity = "\n".join([
            str(product.get("title") or ""),
            str(product.get("category") or ""),
            str(product.get("material_type") or ""),
            str(product.get("description") or ""),
            self._flatten_specs(product.get("specs") or {}),
        ])

        query_brands = self._extract_brand_tokens(query)
        product_brands = self._extract_brand_tokens(product_identity)
        if query_brands and require_brand_match:
            if query_brands & product_brands:
                score += 80
            else:
                return -100

        query_grades = self._extract_grade_tokens(query)
        product_grades = self._extract_grade_tokens(product_identity)
        if query_grades:
            if product_grades and query_grades & product_grades:
                score += 20
            elif product_grades:
                return -100

        query_marks = self._extract_marks(query)
        product_marks = self._extract_marks(
            "\n".join([
                str(product.get("title") or ""),
                self._flatten_specs(product.get("specs") or {}),
            ])
        )
        if query_marks:
            if query_marks & product_marks:
                score += 60
            else:
                return -100

        query_identifiers = self._extract_identifier_tokens(query)
        product_identifiers = self._extract_identifier_tokens(
            "\n".join([
                str(product.get("title") or ""),
                str(product.get("description") or ""),
                self._flatten_specs(product.get("specs") or {}),
            ])
        )
        if query_identifiers:
            if query_identifiers & product_identifiers:
                score += 55
            else:
                return -100

        query_families = self._extract_material_families(query)
        product_families = self._extract_material_families(product_identity)
        if query_families:
            if not product_families:
                return -100
            if self._families_are_compatible(query_families, product_families):
                score += 35
            else:
                return -100

        query_subtypes = self._extract_insulation_subtypes(query)
        if query_subtypes:
            product_subtypes = self._extract_insulation_subtypes(product_identity)
            if not product_subtypes:
                return -100
            if self._insulation_subtypes_are_compatible(query_subtypes, product_subtypes):
                score += 30
            else:
                return -100

        query_surface_features = self._extract_surface_features(query)
        if query_surface_features:
            product_surface_features = self._extract_surface_features(product_identity)
            if not product_surface_features:
                return -100
            if self._surface_features_are_compatible(query_surface_features, product_surface_features):
                score += 18
            else:
                return -100

        query_bases = self._extract_base_types(query)
        product_bases = self._extract_base_types(
            "\n".join([
                str(product.get("title") or ""),
                self._flatten_specs(product.get("specs") or {}),
            ])
        )
        if query_bases and product_bases and query_bases & product_bases:
            score += 25

        query_role = self._extract_roll_role(query)
        product_role = self._extract_roll_role(
            "\n".join([
                str(product.get("title") or ""),
                self._flatten_specs(product.get("specs") or {}),
            ])
        )
        if query_role and product_role == query_role:
            score += 20

        score += min(20, len(product.get("specs") or {}))
        metrics = self._extract_product_metrics(product_payload)
        score += sum(8 for value in metrics.values() if value is not None)
        return score

    def _score_reference_anchor_candidate(self, product: dict, query: str) -> int:
        product_identity = "\n".join([
            str(product.get("title") or ""),
            str(product.get("category") or ""),
            str(product.get("material_type") or ""),
            str(product.get("description") or ""),
            self._flatten_specs(product.get("specs") or {}),
        ])

        query_brands = self._extract_brand_tokens(query)
        product_brands = self._extract_brand_tokens(product_identity)
        if query_brands:
            if not (query_brands & product_brands):
                return -100
            score = 80
        else:
            score = 0

        query_families = self._extract_material_families(query)
        product_families = self._extract_material_families(product_identity)
        if query_families:
            if not product_families or not self._families_are_compatible(query_families, product_families):
                return -100
            score += 35

        query_subtypes = self._extract_insulation_subtypes(query)
        if query_subtypes:
            product_subtypes = self._extract_insulation_subtypes(product_identity)
            if product_subtypes:
                if not self._insulation_subtypes_are_compatible(query_subtypes, product_subtypes):
                    return -100
                score += 30
            else:
                score -= 10

        query_surface_features = self._extract_surface_features(query)
        if query_surface_features:
            product_surface_features = self._extract_surface_features(product_identity)
            if product_surface_features:
                if not self._surface_features_are_compatible(query_surface_features, product_surface_features):
                    return -100
                score += 15
            else:
                score -= 5

        query_marks = self._extract_marks(query)
        product_marks = self._extract_marks(product_identity)
        if query_marks:
            if product_marks:
                if not (query_marks & product_marks):
                    return -100
                score += 35

        query_identifiers = self._extract_identifier_tokens(query)
        product_identifiers = self._extract_identifier_tokens(product_identity)
        if query_identifiers:
            if product_identifiers:
                if not (query_identifiers & product_identifiers):
                    return -100
                score += 45
            else:
                score -= 10

        specs = product.get("specs") or {}
        score += min(10, len(specs) * 2)
        if str(product.get("description") or "").strip():
            score += 5
        return score

    def _build_reference_anchor_profile(
        self,
        query: str,
        candidates: list[dict[str, Any]],
    ) -> Optional[dict[str, Any]]:
        if not self._has_reference_brand_identity(query):
            return None

        best_candidate: Optional[dict[str, Any]] = None
        best_score = 0
        for candidate in candidates:
            score = self._score_reference_anchor_candidate(candidate, query)
            if score > best_score:
                best_score = score
                best_candidate = candidate

        if not best_candidate or best_score < 100:
            return None

        anchor_profile = self._build_reference_profile_from_product(
            best_candidate,
            source="local_reference_anchor",
            strict_price=False,
        )
        query_profile = self._build_query_reference_profile(query)
        if query_profile and self._can_merge_reference_profiles(query, anchor_profile, query_profile):
            anchor_profile = self._merge_reference_profiles(anchor_profile, query_profile)

        anchor_profile["title"] = query.strip() or anchor_profile.get("title", "")
        anchor_profile["source"] = "local_reference_anchor"
        return anchor_profile

    def _build_reference_consensus_profile(
        self,
        query: str,
        candidates: list[dict[str, Any]],
    ) -> Optional[dict[str, Any]]:
        if not self._has_reference_brand_identity(query):
            return None

        query_families = self._extract_material_families(query)
        if not query_families:
            return None

        scored_candidates: list[tuple[int, dict[str, Any], dict[str, Any]]] = []
        for candidate in candidates:
            score = self._score_reference_proxy_candidate(
                candidate,
                query,
                require_brand_match=False,
            )
            if score < 70:
                continue
            profile = self._build_reference_profile_from_product(
                candidate,
                source="local_reference_consensus",
                strict_price=False,
            )
            scored_candidates.append((score, candidate, profile))

        if len(scored_candidates) < 2:
            return None

        query_brands = self._extract_brand_tokens(query)
        query_identifiers = self._extract_identifier_tokens(query)
        if query_identifiers:
            has_scored_identity_anchor = False
            for _, candidate, _ in scored_candidates:
                candidate_identity = "\n".join([
                    str(candidate.get("title") or ""),
                    str(candidate.get("category") or ""),
                    str(candidate.get("material_type") or ""),
                    str(candidate.get("description") or ""),
                    self._flatten_specs(candidate.get("specs") or {}),
                ])
                candidate_brands = self._extract_brand_tokens(candidate_identity)
                candidate_identifiers = self._extract_identifier_tokens(candidate_identity)
                if query_brands and query_brands & candidate_brands:
                    has_scored_identity_anchor = True
                    break
                if query_identifiers & candidate_identifiers:
                    has_scored_identity_anchor = True
                    break
            if not has_scored_identity_anchor:
                return None

        scored_candidates.sort(
            key=lambda item: (
                item[0],
                self._reference_profile_quality(item[2]),
            ),
            reverse=True,
        )
        top_candidates = scored_candidates[:5]

        merged_specs_meta: dict[str, dict[str, Any]] = {}
        metric_values: dict[str, list[float]] = {"thickness": [], "mass": [], "flex": []}
        for rank, (score, candidate, profile) in enumerate(top_candidates):
            specs = profile.get("specs") or {}
            for raw_key, raw_value in specs.items():
                key = str(raw_key or "").strip()
                value = str(raw_value or "").strip()
                if not key or not value:
                    continue
                normalized_key = self._normalize_text(key)
                if not normalized_key:
                    continue
                meta = merged_specs_meta.setdefault(
                    normalized_key,
                    {
                        "label": key,
                        "values": {},
                    },
                )
                value_meta = meta["values"].setdefault(
                    value,
                    {
                        "count": 0,
                        "score": 0,
                    },
                )
                value_meta["count"] += 1
                value_meta["score"] += score - rank

            for metric_name, metric_value in (profile.get("metrics") or {}).items():
                if metric_value is None:
                    continue
                metric_values.setdefault(metric_name, []).append(float(metric_value))

        merged_specs: dict[str, str] = {}
        min_agreement = 2 if len(top_candidates) >= 2 else 1
        for meta in merged_specs_meta.values():
            best_value, best_meta = max(
                meta["values"].items(),
                key=lambda item: (
                    item[1]["count"],
                    item[1]["score"],
                    len(item[0]),
                ),
            )
            if best_meta["count"] < min_agreement:
                continue
            merged_specs[meta["label"]] = best_value

        merged_metrics: dict[str, Optional[float]] = {}
        for metric_name, values in metric_values.items():
            if not values:
                merged_metrics[metric_name] = None
                continue
            values.sort()
            merged_metrics[metric_name] = values[len(values) // 2]

        if not merged_specs and not any(value is not None for value in merged_metrics.values()):
            return None

        return {
            "title": query.strip(),
            "manufacturer": "",
            "price": None,
            "price_unit": "руб",
            "specs": merged_specs,
            "metrics": merged_metrics,
            "families": query_families,
            "marks": self._extract_marks(query),
            "source": "local_reference_consensus",
            "strict_price": False,
        }

    def _build_reference_proxy_profile(
        self,
        query: str,
        candidates: list[dict[str, Any]],
    ) -> Optional[dict[str, Any]]:
        if not self._has_reference_brand_identity(query):
            return None

        query_families = self._extract_material_families(query)
        query_brands = self._extract_brand_tokens(query)
        proxy_min_score = 70
        if query_families & {"primer", "sealant", "profnastil", "membrane", "insulation", "glassfiber_roll"}:
            proxy_min_score = 40

        scored_candidates: list[tuple[int, dict[str, Any], dict[str, Any]]] = []
        for candidate in candidates:
            score = self._score_reference_proxy_candidate(candidate, query)
            if score < proxy_min_score:
                continue
            profile = self._build_reference_profile_from_product(
                candidate,
                source="local_reference_proxy",
                strict_price=False,
            )
            scored_candidates.append((score, candidate, profile))

        min_candidates = 1 if query_brands else 2
        if len(scored_candidates) < min_candidates:
            return None

        scored_candidates.sort(
            key=lambda item: (
                item[0],
                self._reference_profile_quality(item[2]),
            ),
            reverse=True,
        )
        top_candidates = scored_candidates[:5]

        merged_specs_meta: dict[str, dict[str, Any]] = {}
        metric_values: dict[str, list[float]] = {"thickness": [], "mass": [], "flex": []}
        manufacturers: list[str] = []

        for rank, (score, candidate, profile) in enumerate(top_candidates):
            specs = profile.get("specs") or {}
            for raw_key, raw_value in specs.items():
                key = str(raw_key or "").strip()
                value = str(raw_value or "").strip()
                if not key or not value:
                    continue
                normalized_key = self._normalize_text(key)
                if not normalized_key:
                    continue
                meta = merged_specs_meta.setdefault(
                    normalized_key,
                    {"label": key, "label_score": score, "values": {}},
                )
                if score > meta["label_score"]:
                    meta["label"] = key
                    meta["label_score"] = score
                value_meta = meta["values"].setdefault(
                    value,
                    {"count": 0, "score": -1},
                )
                value_meta["count"] += 1
                value_meta["score"] = max(value_meta["score"], score - rank)

            for metric_name, metric_value in (profile.get("metrics") or {}).items():
                if metric_value is not None:
                    metric_values.setdefault(metric_name, []).append(float(metric_value))

            manufacturer = str(profile.get("manufacturer") or "").strip()
            if manufacturer:
                manufacturers.append(manufacturer)

        merged_specs: dict[str, str] = {}
        for meta in merged_specs_meta.values():
            best_value = max(
                meta["values"].items(),
                key=lambda item: (
                    item[1]["count"],
                    item[1]["score"],
                    len(item[0]),
                ),
            )[0]
            merged_specs[meta["label"]] = best_value

        merged_metrics: dict[str, Optional[float]] = {}
        for metric_name, values in metric_values.items():
            if not values:
                merged_metrics[metric_name] = None
                continue
            values.sort()
            merged_metrics[metric_name] = values[len(values) // 2]

        manufacturer = ""
        if manufacturers:
            manufacturer_counts: dict[str, int] = {}
            for value in manufacturers:
                manufacturer_counts[value] = manufacturer_counts.get(value, 0) + 1
            manufacturer = max(
                manufacturer_counts.items(),
                key=lambda item: item[1],
            )[0]

        return {
            "title": query.strip(),
            "manufacturer": manufacturer,
            "price": None,
            "price_unit": "руб",
            "specs": merged_specs,
            "metrics": merged_metrics,
            "families": self._extract_material_families(query),
            "marks": self._extract_marks(query),
            "source": "local_reference_proxy",
            "strict_price": False,
        }

    def _public_reference_profile(self, reference_profile: Optional[dict[str, Any]]) -> dict[str, Any] | None:
        if not isinstance(reference_profile, dict) or not reference_profile:
            return None

        specs = reference_profile.get("specs")
        metrics = reference_profile.get("metrics")
        return {
            "title": reference_profile.get("title", ""),
            "manufacturer": reference_profile.get("manufacturer", ""),
            "price": reference_profile.get("price"),
            "price_unit": reference_profile.get("price_unit", "") or "руб",
            "specs": specs if isinstance(specs, dict) else {},
            "metrics": metrics if isinstance(metrics, dict) else {},
            "source": reference_profile.get("source", ""),
            "strict_price": bool(reference_profile.get("strict_price")),
        }

    def _resolve_reference_profile_local(self, query: str) -> Optional[dict[str, Any]]:
        if not self._has_explicit_reference_identity(query):
            return None
        tokens = self._build_reference_lookup_tokens(query)
        if not tokens:
            return None

        from sqlalchemy import text

        try:
            with self.db_session_factory() as session:
                candidate_ids = self._load_candidate_ids_from_fts(session, [query, *tokens], limit=400)
                rows = list(
                    self._load_local_search_rows(
                        session,
                        limit=1200 if candidate_ids else 2500,
                        candidate_ids=candidate_ids or None,
                    )
                )
                if candidate_ids and len(rows) < 300:
                    fallback_rows = self._load_local_search_rows(
                        session,
                        limit=1200,
                        candidate_ids=None,
                    )
                    seen_ids = {row.id for row in rows}
                    for row in fallback_rows:
                        if row.id not in seen_ids:
                            rows.append(row)
                            seen_ids.add(row.id)
                elif not candidate_ids:
                    rows = list(rows)
        except Exception as e:
            logger.warning(f"[AnalogService] Failed to resolve local reference profile: {e}")
            return None

        scored_candidates: list[dict[str, Any]] = []
        best_profile = None
        best_score = 0
        for row in rows:
            product = {
                "id": row.id,
                "title": row.title,
                "category": row.category,
                "material_type": row.material_type,
                "price": row.price,
                "specs": {},
                "url": self._normalize_external_url(row.url),
                "description": row.description,
            }
            if row.specs:
                try:
                    product["specs"] = json.loads(row.specs) if isinstance(row.specs, str) else row.specs
                except Exception:
                    product["specs"] = {}

            score = self._score_reference_candidate(product, query)
            scored_candidates.append({"product": product, "score": score})
            if score > best_score:
                best_score = score
                best_profile = self._build_reference_profile_from_product(
                    product,
                    source="local_reference",
                    strict_price=score >= 220,
                )

        local_profile = best_profile if best_score >= 110 else None
        proxy_profile = self._build_reference_proxy_profile(
            query,
            [entry["product"] for entry in scored_candidates],
        )
        consensus_profile = self._build_reference_consensus_profile(
            query,
            [entry["product"] for entry in scored_candidates],
        )
        anchor_profile = self._build_reference_anchor_profile(
            query,
            [entry["product"] for entry in scored_candidates],
        )

        if local_profile and proxy_profile:
            return self._merge_reference_profiles(local_profile, proxy_profile)
        if local_profile:
            return local_profile
        if proxy_profile:
            return proxy_profile
        if consensus_profile:
            return consensus_profile
        return anchor_profile

    def _resolve_reference_profile_ai(self, query: str, requirements: str = "") -> Optional[dict[str, Any]]:
        if not self.ai_service or not hasattr(self.ai_service, "generate_with_search"):
            return None
        if self._is_ai_temporarily_blocked("reference"):
            return None

        prompt = f"""Ты технический специалист по строительным и гидроизоляционным материалам.

Нужно определить РЕФЕРЕНСНЫЙ профиль исходного товара по его названию/марке/артикулу.
Исходное название может быть коммерческим. Используй бренд, серию и артикул только для поиска точных характеристик исходного товара.

ИСХОДНЫЙ ТОВАР:
{query}

ДОПОЛНИТЕЛЬНЫЕ ТРЕБОВАНИЯ ИЗ ТЗ:
{(requirements or '')[:1500]}

Верни ТОЛЬКО JSON:
{{
  "resolved": true,
  "reference_title": "точное название исходного товара",
  "manufacturer": "производитель",
  "price": null,
  "price_unit": "руб/рулон",
  "specs": {{
    "Толщина, мм": "значение",
    "Масса, кг/м2": "значение",
    "Температура гибкости на брусе R=25, °C, не выше": "значение",
    "Основа": "значение"
  }},
  "notes": ["краткие замечания по достоверности"]
}}

Если точный профиль определить нельзя, верни:
{{"resolved": false, "notes": ["почему не удалось"]}}

Не выдумывай цену. Если достоверную цену найти не удалось, верни "price": null.
"""

        try:
            response_text = self.ai_service.generate_with_search(prompt)
            payload = self._parse_first_json_object(response_text)
        except Exception as e:
            error_str = str(e)
            if (
                "429" in error_str
                or "RESOURCE_EXHAUSTED" in error_str
                or "QUOTA_EXHAUSTED" in error_str
                or "SERVICE_UNAVAILABLE" in error_str
                or "Empty response text from Gemini" in error_str
                or "Empty response object from Gemini" in error_str
            ):
                retry_after = self._extract_retry_after_seconds(error_str)
                self._block_ai_temporarily(
                    seconds=retry_after + 1.0 if retry_after else 10.0,
                    reason="reference_profile_lookup_failed",
                    scope="reference",
                )
            logger.warning(f"[AnalogService] AI reference profile lookup failed: {e}")
            return None

        if not payload.get("resolved"):
            return None

        product_payload = {
            "title": payload.get("reference_title", query),
            "category": payload.get("material_type", ""),
            "material_type": payload.get("material_type", ""),
            "description": " ; ".join(payload.get("notes") or []),
            "specs": payload.get("specs") or {},
        }
        identity_source = "\n".join([
            str(payload.get("reference_title", query) or ""),
            str(payload.get("manufacturer") or ""),
            str(payload.get("material_type") or ""),
            self._flatten_specs(payload.get("specs") or {}),
            " ; ".join(payload.get("notes") or []),
        ])
        profile = {
            "title": payload.get("reference_title", query),
            "manufacturer": payload.get("manufacturer", ""),
            "price": self._parse_price_value(payload.get("price")),
            "price_unit": payload.get("price_unit", ""),
            "specs": payload.get("specs") or {},
            "metrics": self._extract_product_metrics(product_payload),
            "families": self._extract_material_families(identity_source),
            "marks": self._extract_marks(identity_source),
            "source": "ai_reference",
            "notes": payload.get("notes") or [],
            "strict_price": self._parse_price_value(payload.get("price")) is not None,
        }
        if self._has_reference_brand_identity(query) and not self._is_reference_profile_exact_match(profile, query):
            logger.warning(
                "[AnalogService] AI reference profile rejected as incompatible with query: query=%r title=%r",
                query,
                profile.get("title"),
            )
            return None
        return profile

    def _resolve_reference_profile(
        self,
        query: str,
        requirements: str = "",
        *,
        allow_ai_lookup: bool = True,
    ) -> Optional[dict[str, Any]]:
        cache_key = (
            f"{'full' if allow_ai_lookup else 'local'}|"
            f"{self._clean_search_query(query)}|{(requirements or '').strip()}"
        )
        if cache_key in self._reference_profile_cache:
            return self._reference_profile_cache[cache_key]

        profile = self._resolve_reference_profile_local(query)

        self._reference_profile_cache[cache_key] = profile
        return profile

    def _query_requires_reference_lookup(
        self,
        query: str,
        requirements: str = "",
    ) -> bool:
        normalized_query = (query or "").strip()
        if not normalized_query:
            return False
        return self._has_reference_brand_identity(normalized_query)

    def _augment_requirements_with_reference(
        self,
        requirements: str | None,
        reference_profile: Optional[dict[str, Any]],
    ) -> str:
        base = (requirements or "").strip()
        if not reference_profile:
            return base

        metrics = reference_profile.get("metrics") or {}
        lines = []
        if reference_profile.get("title"):
            lines.append(f"Референсный товар: {reference_profile['title']}")
        if reference_profile.get("price") is not None:
            price_value = int(round(float(reference_profile["price"])))
            lines.append(f"Цена исходного товара не выше {price_value} руб")
        if metrics.get("thickness") is not None:
            lines.append(f"Толщина исходного товара: {metrics['thickness']} мм")
        if metrics.get("mass") is not None:
            lines.append(f"Масса исходного товара: {metrics['mass']} кг/м2")
        if metrics.get("flex") is not None:
            lines.append(f"Температура гибкости на брусе R=25, °C: {metrics['flex']}")
        specs = reference_profile.get("specs") or {}
        if specs.get("Основа"):
            lines.append(f"Основа исходного товара: {specs['Основа']}")

        if not lines:
            return base

        addition = "РЕФЕРЕНСНЫЙ ПРОФИЛЬ ИСХОДНОГО ТОВАРА:\n" + "\n".join(lines)
        return f"{base}\n\n{addition}".strip() if base else addition

    def _tokenize(self, text: str) -> list[str]:
        tokens = []
        for token in self._normalize_text(text).split():
            token = token.strip(".,")
            if self._is_noise_token(token):
                continue
            if token not in tokens:
                tokens.append(token)
        return tokens

    def _flatten_specs(self, specs: dict) -> str:
        if not specs:
            return ""
        parts = []
        for key, value in specs.items():
            parts.append(f"{key}: {value}")
        return " ; ".join(parts)

    def _build_product_search_text(self, product: dict) -> str:
        return self._normalize_text(
            " ".join(
                [
                    str(product.get("title") or ""),
                    str(product.get("category") or ""),
                    str(product.get("material_type") or ""),
                    str(product.get("material_group") or ""),
                    str(product.get("product_family") or ""),
                    str(product.get("base_material") or ""),
                    str(product.get("description") or ""),
                    self._flatten_specs(product.get("specs") or {}),
                    str(product.get("search_text") or ""),
                ]
            )
        )

    def _extract_product_manufacturer(self, product: dict) -> str:
        direct_value = str(product.get("manufacturer") or "").strip()
        if direct_value:
            return direct_value

        specs = product.get("specs") or {}
        for key, value in specs.items():
            if re.search(r"производител", str(key or ""), flags=re.IGNORECASE):
                candidate = str(value or "").strip()
                if candidate:
                    return candidate

        description = str(product.get("description") or "")
        match = re.search(
            r"производител[^\n\r:]{0,20}:\s*([^;,\n\r]+)",
            description,
            flags=re.IGNORECASE,
        )
        if match:
            return match.group(1).strip()

        return ""

    def _product_matches_keywords(
        self,
        product: dict,
        keywords: list[str],
    ) -> tuple[bool, list[str]]:
        if not keywords:
            return True, []

        search_text = self._build_product_search_text(product)
        matched_keywords = self._match_keywords_in_text(keywords, search_text)
        return bool(matched_keywords), matched_keywords

    @staticmethod
    def _keyword_stem(keyword: str) -> str:
        normalized = str(keyword or "").strip().lower()
        if not normalized or " " in normalized:
            return normalized

        special_stems = {
            "стеклоткань": "стеклоткан",
            "стеклоткани": "стеклоткан",
            "стеклотканью": "стеклоткан",
            "стеклотканям": "стеклоткан",
            "стеклохолст": "стеклохолст",
            "стеклохолста": "стеклохолст",
            "стеклохолсте": "стеклохолст",
            "полиэстер": "полиэстер",
            "полиэстера": "полиэстер",
            "полиэстером": "полиэстер",
        }
        if normalized in special_stems:
            return special_stems[normalized]

        endings = (
            "ыми", "ими", "ого", "ему", "ому", "ыми", "ими",
            "ыми", "иях", "иях", "ыми", "ыми", "ами", "ями",
            "ией", "ией", "иям", "иях", "ией",
            "ый", "ий", "ой", "ая", "яя", "ое", "ее", "ые", "ие",
            "ых", "их", "ую", "юю", "ым", "им", "ом", "ем", "ой", "ей",
            "ам", "ям", "ах", "ях", "а", "я", "ы", "и", "е", "у", "ю",
        )
        for ending in endings:
            if normalized.endswith(ending) and len(normalized) - len(ending) >= 5:
                return normalized[: -len(ending)]
        return normalized

    def _keyword_match_variants(self, keyword: str) -> list[str]:
        normalized = self._normalize_text(keyword).strip()
        if not normalized:
            return []

        variants: list[str] = []

        def add_variant(value: str) -> None:
            normalized_value = self._normalize_text(value).strip()
            if not normalized_value or normalized_value in variants:
                return
            variants.append(normalized_value)

        add_variant(normalized)

        stem = self._keyword_stem(normalized)
        add_variant(stem)

        if re.fullmatch(r"\d{2,4}", normalized):
            return variants
        if re.fullmatch(r"\d{2,3}кг", normalized):
            add_variant(normalized[:-2] + " кг")
            return variants
        if re.fullmatch(r"\d{2,3}/\d{1,2}", normalized):
            add_variant(normalized.replace("/", " "))
            return variants

        alias_variants = {
            "стеклоткан": ["стеклоткан", "с т"],
            "стеклохолст": ["стеклохолст", "с х"],
            "полиэстер": ["полиэстер", "п э"],
            "крупнозернист": ["крупнозернист"],
            "мелкозернист": ["мелкозернист"],
            "посыпк": ["посыпк"],
            "пленк": ["пленк"],
            "геотекст": ["геотекст", "дорнит", "пфг", "иглопробив", "фильерн", "полиэфирн"],
            "геомембран": ["геомембран", "изостуд", "дрениз", "plastguard", "planter", "профилированн мембран"],
            "битум": ["битум", "брикет"],
            "гидрошпонк": ["гидрошпонк", "waterstop", "пенебар", "гидропроклад"],
        }
        for root, aliases in alias_variants.items():
            if normalized.startswith(root) or stem.startswith(root):
                for alias in aliases:
                    add_variant(alias)

        return variants

    def _match_keywords_in_text(self, keywords: Sequence[str], text: str) -> list[str]:
        normalized_text = self._normalize_text(text)
        if not normalized_text:
            return []

        matched: list[str] = []
        for keyword in keywords:
            variants = self._keyword_match_variants(keyword)
            if any(variant in normalized_text for variant in variants):
                matched.append(keyword)
        return matched

    def _extract_marks(self, text: str) -> set[str]:
        text = self._normalize_text(text)
        return set(re.findall(r"\b(тпп|ткп|хпп|хкп|эпп|экп|эмп)\b", text, flags=re.IGNORECASE))

    def _extract_material_families(self, text: str) -> set[str]:
        normalized = self._normalize_text(text)
        if not normalized:
            return set()

        family_patterns = {
            "primer": [r"\bпраймер\b"],
            "mastika": [r"\bмастик"],
            "ruberoid": [r"\bрубероид\b", r"\bрпп[-\s]?\d", r"\bркк[-\s]?\d", r"\bркп[-\s]?\d"],
            "bitumen": [r"\bбитум\b", r"\bбн\s*90[ /-]?10\b", r"\b90\s*[/-]\s*10\b"],
            "bitumen_roll": [
                r"\bнаплавля", r"\bгидроизол\b", r"\bстеклоизол\b", r"\bгидростеклоизол\b",
                r"\bтехноэласт\b", r"\bунифлекс\b", r"\bбипол\b", r"\bбикрост\b", r"\bлинокром\b",
                r"\bфилизол\b", r"\bизоэласт\b", r"\bэластоизол\b", r"\bэластобит\b",
                r"\bстеклоэласт\b", r"\bстеклофлекс\b", r"\bрубитэкс\b", r"\bмостослой\b",
            ],
            "glassfiber_roll": [r"\bстеклопласт", r"\bрст[-\s]?\d"],
            "pvc_membrane": [r"\bпвх\b", r"\bpvc\b"],
            "tpo_membrane": [r"\btpo\b"],
            "epdm_membrane": [r"\bepdm\b"],
            "membrane": [r"\bмембран"],
            "geomembrane": [
                r"\bгеомембран",
                r"\bизостуд\b",
                r"\bдрениз\b",
                r"\bplastguard\b",
                r"\bplanter\b",
                r"\bпрофилированн\w+\s+мембран",
                r"\bдренажн\w+\s+мембран",
            ],
            "geotextile": [
                r"\bгеотекст",
                r"\bдорнит\b",
                r"\bпфг\b",
                r"\bиглопробив",
                r"\bфильерн",
            ],
            "film": [r"\bпленк", r"\bпароизоляц", r"\bармированн\w+\s+пленк"],
            "tape": [r"\bлента\b", r"\bгерлен\b"],
            "sealant": [r"\bгерметик"],
            "xps": [r"\bxps\b", r"\bпеноплэкс\b", r"\bэкструдированн\w+\s+пенополистир"],
            "insulation": [
                r"\bутеплител",
                r"\bпенополистир",
                r"\bpenoplex\b",
                r"\bминват",
                r"\bминеральн\w+\s+вата",
                r"\bfastfix\b",
            ],
            "shponka": [r"\bшпонк"],
            "profnastil": [r"\bпрофнастил\b", r"\bс-\d+\b", r"\bнс-\d+\b"],
        }

        families = {
            family
            for family, patterns in family_patterns.items()
            if any(re.search(pattern, normalized, flags=re.IGNORECASE) for pattern in patterns)
        }

        if (
            re.search(r"\bрулонн", normalized, flags=re.IGNORECASE)
            and re.search(r"\bбитум", normalized, flags=re.IGNORECASE)
            and not re.search(r"\bлента\b|\bгерметик\b|\bмастик\b|\bпраймер\b", normalized, flags=re.IGNORECASE)
        ):
            families.add("bitumen_roll")

        if self._extract_marks(normalized):
            families.add("bitumen_roll")
        if "ruberoid" in families:
            families.add("bitumen_roll")
        if {"pvc_membrane", "tpo_membrane", "epdm_membrane"} & families:
            families.add("membrane")

        return families

    def _extract_insulation_subtypes(self, text: str) -> set[str]:
        normalized = self._normalize_text(text)
        if not normalized:
            return set()

        subtypes = set()
        if re.search(
            r"\bминват\b|\bстекловат\b|\bкаменн\w+\s+вата\b|\bминеральн\w+\b",
            normalized,
            flags=re.IGNORECASE,
        ):
            subtypes.add("mineral_wool")
        if re.search(
            r"\bxps\b|\bпеноплэкс\b|\bэкструдированн\w+\s+пенополистир",
            normalized,
            flags=re.IGNORECASE,
        ):
            subtypes.add("xps")
        if re.search(
            r"\bвспененн\w+\s+полиэтилен\b|\bнпэ\b|\bпенофол\b|\bтепофол\b",
            normalized,
            flags=re.IGNORECASE,
        ):
            subtypes.add("pe_foam")
        if re.search(
            r"\bнапыляем\w+\s+утеплител\b|\bfastfix\b",
            normalized,
            flags=re.IGNORECASE,
        ):
            subtypes.add("spray_foam")
        return subtypes

    def _extract_surface_features(self, text: str) -> set[str]:
        normalized = self._normalize_text(text)
        if not normalized:
            return set()

        features = set()
        marks = self._extract_marks(normalized)
        if re.search(r"\bфольг|\bфольга\b|\bалюмини", normalized, flags=re.IGNORECASE):
            features.add("foil")
        if re.search(r"\bметаллиз", normalized, flags=re.IGNORECASE):
            features.add("metallized")
        if re.search(r"\bпленк", normalized, flags=re.IGNORECASE) or marks & {"тпп", "эпп", "хпп", "эмп"}:
            features.add("film")
        if (
            re.search(r"\bкрупнозернист", normalized, flags=re.IGNORECASE)
            or re.search(r"\bсланц", normalized, flags=re.IGNORECASE)
            or marks & {"ткп", "экп", "хкп"}
        ):
            features.add("coarse_granule")
        if re.search(r"\bмелкозернист", normalized, flags=re.IGNORECASE) or re.search(
            r"\bпес(?:ок|чан)\b",
            normalized,
            flags=re.IGNORECASE,
        ):
            features.add("fine_granule")
        return features

    def _extract_base_types(self, text: str) -> set[str]:
        normalized = self._normalize_text(text)
        bases = set()
        marks = self._extract_marks(normalized)

        if re.search(r"\bполиэстер\b|\bп/э\b", normalized, flags=re.IGNORECASE) or marks & {"эпп", "экп", "эмп"}:
            bases.add("polyester")
        if re.search(r"\bстеклоткан", normalized, flags=re.IGNORECASE) or marks & {"тпп", "ткп"}:
            bases.add("fiberglass_tissue")
        if re.search(r"\bстеклохолст", normalized, flags=re.IGNORECASE) or marks & {"хпп", "хкп"}:
            bases.add("fiberglass_mat")
        if re.search(r"\bкартон\b|\bрпп\b|\bркк\b|\bркп\b", normalized, flags=re.IGNORECASE):
            bases.add("cardboard")

        return bases

    def _extract_application_methods(self, text: str) -> set[str]:
        normalized = self._normalize_text(text)
        methods = set()

        if re.search(r"\bнаплавля", normalized, flags=re.IGNORECASE):
            methods.add("torch_applied")
        if re.search(r"\bсамокле", normalized, flags=re.IGNORECASE):
            methods.add("self_adhesive")
        if re.search(r"\bхолодн\w+\s+нанес", normalized, flags=re.IGNORECASE):
            methods.add("cold_applied")

        return methods

    def _extract_roll_role(self, text: str) -> Optional[str]:
        normalized = self._normalize_text(text)
        marks = self._extract_marks(normalized)

        if marks & {"ткп", "экп", "хкп"} or re.search(
            r"\bверхн\w*\s+сло|\bкрупнозернист|\bсланц",
            normalized,
            flags=re.IGNORECASE,
        ):
            return "top"
        if marks & {"тпп", "эпп", "хпп", "эмп"} or re.search(r"\bподкладоч|\bнижн", normalized, flags=re.IGNORECASE):
            return "underlay"

        return None

    def _extract_requirement_constraint(
        self,
        text: str,
        label_patterns: list[str],
        unit_patterns: list[str],
        exclude_patterns: Optional[list[str]] = None,
        allow_unit_only_fallback: bool = False,
    ) -> Optional[dict[str, Any]]:
        if not text:
            return None

        normalized = str(text).lower().replace(",", ".")
        labels = "|".join(label_patterns)
        units = "|".join(unit_patterns)

        lines = [line.strip() for line in re.split(r"[\n\r]+", normalized) if line.strip()] or [normalized]
        for line in lines:
            if exclude_patterns and any(re.search(pattern, line, flags=re.IGNORECASE) for pattern in exclude_patterns):
                continue
            if not re.search(rf"(?:{labels})", line, flags=re.IGNORECASE):
                continue

            regexes = [
                (rf"(?:{labels})[^\n\r]{{0,35}}?(не\s+менее|не\s+ниже|от)\s*(-?\d+(?:\.\d+)?)", "min"),
                (rf"(?:{labels})[^\n\r]{{0,35}}?(не\s+более|не\s+выше|до)\s*(-?\d+(?:\.\d+)?)", "max"),
                (rf"(?:{labels})[^\n\r]{{0,35}}?(-?\d+(?:\.\d+)?)\s*(?:{units})", "target"),
                (rf"(?:{labels})[^\n\r]{{0,35}}?(-?\d+(?:\.\d+)?)", "target"),
            ]

            if allow_unit_only_fallback:
                regexes.append((rf"(-?\d+(?:\.\d+)?)\s*(?:{units})", "target"))

            for pattern, mode in regexes:
                match = re.search(pattern, line, flags=re.IGNORECASE)
                if not match:
                    continue
                raw_value = match.group(match.lastindex).replace(",", ".")
                try:
                    return {"value": float(raw_value), "mode": mode}
                except Exception:
                    continue

        return None

    def _extract_inline_unit_constraint(
        self,
        text: str,
        unit_patterns: list[str],
        *,
        allow_negative: bool = False,
    ) -> Optional[dict[str, Any]]:
        if not text:
            return None

        normalized = str(text).lower().replace(",", ".")
        units = "|".join(unit_patterns)
        value_pattern = r"-?\d+(?:\.\d+)?" if allow_negative else r"\d+(?:\.\d+)?"
        lines = [line.strip() for line in re.split(r"[\n\r]+", normalized) if line.strip()] or [normalized]

        regexes = [
            (rf"(?:не\s+менее|не\s+ниже|от)\s*({value_pattern})\s*(?:{units})", "min"),
            (rf"({value_pattern})\s*(?:{units})[^\n\r]{{0,20}}?(?:не\s+менее|не\s+ниже)", "min"),
            (rf"(?:не\s+более|не\s+выше|до)\s*({value_pattern})\s*(?:{units})", "max"),
            (rf"({value_pattern})\s*(?:{units})[^\n\r]{{0,20}}?(?:не\s+более|не\s+выше)", "max"),
            (rf"({value_pattern})\s*(?:{units})", "target"),
        ]

        for line in lines:
            for pattern, mode in regexes:
                match = re.search(pattern, line, flags=re.IGNORECASE)
                if not match:
                    continue
                raw_value = match.group(1).replace(",", ".")
                try:
                    return {"value": float(raw_value), "mode": mode}
                except Exception:
                    continue

        return None

    def _extract_requirement_constraints(self, query: str, requirements: str = "") -> dict[str, Optional[dict[str, Any]]]:
        source = f"{query}\n{requirements}"
        thickness_constraint = self._extract_requirement_constraint(
            source,
            [r"толщин[аы]?"],
            [r"мм"],
        )
        if thickness_constraint is None:
            thickness_constraint = self._extract_inline_unit_constraint(
                source,
                [r"мм"],
            )

        mass_constraint = self._extract_requirement_constraint(
            source,
            [r"\bмасс[аы]?\b", r"\bвес\b"],
            [r"кг/?м2", r"кг/?м²"],
            exclude_patterns=[r"массов\w+\s+дол"],
        )
        if mass_constraint is None:
            mass_constraint = self._extract_inline_unit_constraint(
                source,
                [r"кг/?м2", r"кг/?м²"],
            )

        return {
            "thickness": thickness_constraint,
            "mass": mass_constraint,
            "flex": self._extract_requirement_constraint(
                source,
                [r"гибкост[ьи]", r"температур[аы][^\n\r]{0,12}гибкост", r"температур[аы]\s+хрупкост"],
                [r"°?c", r"°?с"],
            ),
        }

    def _extract_numeric_values(self, text: Any, allow_negative: bool = False) -> list[float]:
        normalized = str(text or "").replace(",", ".").replace("\xa0", " ").strip()
        if not normalized:
            return []

        pattern = r"(?<!\d)-?\d+(?:\.\d+)?" if allow_negative else r"(?<![\d-])\d+(?:\.\d+)?"
        values: list[float] = []
        for match in re.findall(pattern, normalized):
            try:
                values.append(float(match))
            except Exception:
                continue
        return values

    def _extract_metric_from_specs(
        self,
        specs: dict,
        label_patterns: list[str],
        *,
        exclude_patterns: Optional[list[str]] = None,
        allow_negative: bool = False,
    ) -> Optional[float]:
        for key, value in (specs or {}).items():
            key_text = str(key or "").lower().replace(",", ".")
            if exclude_patterns and any(re.search(pattern, key_text, flags=re.IGNORECASE) for pattern in exclude_patterns):
                continue
            if not any(re.search(pattern, key_text, flags=re.IGNORECASE) for pattern in label_patterns):
                continue

            value_numbers = self._extract_numeric_values(value, allow_negative=allow_negative)
            if value_numbers:
                return value_numbers[-1]

            combined_numbers = self._extract_numeric_values(f"{key}: {value}", allow_negative=allow_negative)
            if combined_numbers:
                return combined_numbers[-1]

        return None

    def _extract_product_metrics(self, product: dict) -> dict[str, Optional[float]]:
        specs = product.get("specs") or {}
        specs_text = self._flatten_specs(specs)
        description = str(product.get("description") or "")

        return {
            "thickness": (
                self._extract_metric_from_specs(
                    specs,
                    [r"толщин[аы]?"],
                )
                or self._extract_first_number_after_label(specs_text, [
                    r"толщин[^\n\r:]{0,80}:\s*(-?\d+(?:\.\d+)?)\s*мм",
                ])
                or self._extract_first_number_after_label(description, [
                    r"толщин[^\n\r]{0,40}?(-?\d+(?:\.\d+)?)\s*мм",
                ])
            ),
            "mass": (
                self._extract_metric_from_specs(
                    specs,
                    [r"\bмасс[аы]?\b", r"плотност"],
                    exclude_patterns=[r"массов\w+\s+дол"],
                )
                or self._extract_first_number_after_label(specs_text, [
                    r"(?:масс[аы]?|плотност[а-я]*)[^\n\r:]{0,80}:\s*(-?\d+(?:\.\d+)?)\s*кг",
                ])
                or self._extract_first_number_after_label(description, [
                    r"(?:масс[аы]?|плотност[а-я]*)[^\n\r]{0,40}?(-?\d+(?:\.\d+)?)\s*кг(?:\s*/?\s*(?:м2|м²|кв\.?\s*м))?",
                ])
            ),
            "flex": (
                self._extract_metric_from_specs(
                    specs,
                    [r"гибкост[ьи]", r"температур[аы]\s+гибкост", r"температур[аы]\s+хрупкост"],
                    allow_negative=True,
                )
                or self._extract_first_number_after_label(specs_text, [
                    r"(?:гибкост[ьи]|температур[аы]\s+гибкост[ьи]?|температур[аы]\s+хрупкост[ьи]?)[^\n\r:]{0,80}:\s*(-?\d+(?:\.\d+)?)",
                ])
                or self._extract_first_number_after_label(description, [
                    r"(?:гибкост[ьи]|температур[аы]\s+гибкост[ьи]?|температур[аы]\s+хрупкост[ьи]?)[^\n\r]{0,40}?(-?\d+(?:\.\d+)?)\s*°?[cс]",
                ])
            ),
        }

    def _families_are_compatible(self, query_families: set[str], product_families: set[str]) -> bool:
        if not query_families:
            return True
        if not product_families:
            return False

        compatibility_map = {
            "primer": {"primer"},
            "mastika": {"mastika"},
            "bitumen": {"bitumen"},
            "ruberoid": {"ruberoid"},
            "bitumen_roll": {"bitumen_roll", "ruberoid"},
            "glassfiber_roll": {"glassfiber_roll"},
            "pvc_membrane": {"pvc_membrane"},
            "tpo_membrane": {"tpo_membrane"},
            "epdm_membrane": {"epdm_membrane"},
            "membrane": {"membrane", "pvc_membrane", "tpo_membrane", "epdm_membrane"},
            "geomembrane": {"geomembrane"},
            "geotextile": {"geotextile"},
            "film": {"film"},
            "tape": {"tape"},
            "sealant": {"sealant"},
            "xps": {"xps", "insulation"},
            "insulation": {"insulation", "xps"},
            "shponka": {"shponka"},
            "profnastil": {"profnastil"},
        }

        specific_query_families = set(query_families)
        if {"pvc_membrane", "tpo_membrane", "epdm_membrane"} & specific_query_families:
            specific_query_families.discard("membrane")
        if "geomembrane" in specific_query_families:
            specific_query_families.discard("membrane")

        for family in specific_query_families:
            if product_families & compatibility_map.get(family, {family}):
                return True

        return False

    def _insulation_subtypes_are_compatible(
        self,
        query_subtypes: set[str],
        product_subtypes: set[str],
    ) -> bool:
        if not query_subtypes:
            return True
        if not product_subtypes:
            return False

        compatibility_map = {
            "mineral_wool": {"mineral_wool"},
            "xps": {"xps"},
            "pe_foam": {"pe_foam"},
            "spray_foam": {"spray_foam"},
        }
        for subtype in query_subtypes:
            if product_subtypes & compatibility_map.get(subtype, {subtype}):
                return True
        return False

    def _surface_features_are_compatible(
        self,
        query_features: set[str],
        product_features: set[str],
    ) -> bool:
        if not query_features:
            return True
        if not product_features:
            return False

        if "coarse_granule" in query_features and "fine_granule" in product_features:
            return False
        if "fine_granule" in query_features and "coarse_granule" in product_features:
            return False

        compatibility_map = {
            "foil": {"foil", "metallized"},
            "metallized": {"foil", "metallized"},
            "film": {"film"},
            "coarse_granule": {"coarse_granule"},
            "fine_granule": {"fine_granule"},
        }
        for feature in query_features:
            if product_features & compatibility_map.get(feature, {feature}):
                return True
        return False

    @staticmethod
    def _metric_label(metric_name: str) -> str:
        return {
            "thickness": "толщина",
            "mass": "масса",
            "flex": "гибкость",
        }.get(metric_name, metric_name)

    def _passes_numeric_constraint(
        self,
        metric_name: str,
        constraint: Optional[dict[str, Any]],
        product_value: Optional[float],
    ) -> tuple[bool, Optional[str]]:
        if constraint is None:
            return True, None

        if product_value is None:
            return True, f"параметр '{self._metric_label(metric_name)}' не подтвержден"

        value = float(constraint["value"])
        mode = constraint["mode"]

        tolerances = {
            "thickness": 0.25,
            "mass": 0.15,
            "flex": 4.0,
        }
        tolerance = tolerances.get(metric_name, 0.0)

        if metric_name == "flex":
            if mode == "max" and product_value > value + tolerance:
                return False, "температура гибкости хуже требования"
            if mode == "min" and product_value < value - tolerance:
                return False, "температура гибкости не соответствует требованию"
            if mode == "target" and abs(product_value - value) > max(tolerance * 2, 8):
                return False, "температура гибкости слишком отличается"
            return True, None

        if mode == "min" and product_value + tolerance < value:
            return False, f"параметр '{self._metric_label(metric_name)}' ниже требования"
        if mode == "max" and product_value - tolerance > value:
            return False, f"параметр '{self._metric_label(metric_name)}' выше допустимого"
        if mode == "target" and abs(product_value - value) > tolerance * 2:
            return False, f"параметр '{self._metric_label(metric_name)}' слишком отличается"

        return True, None

    def _passes_hard_filters(
        self,
        product: dict,
        query: str,
        requirements: str = "",
        reference_profile: Optional[dict[str, Any]] = None,
    ) -> tuple[bool, list[str]]:
        query_source = f"{query}\n{requirements}"
        product_identity_source = "\n".join([
            str(product.get("title") or ""),
            str(product.get("category") or ""),
            str(product.get("material_type") or ""),
        ])
        product_facts_source = "\n".join([
            product_identity_source,
            self._flatten_specs(product.get("specs") or {}),
        ])
        product_source = "\n".join([
            product_facts_source,
            str(product.get("description") or ""),
        ])
        product_title_normalized = self._normalize_text(product.get("title", ""))
        product_manufacturer = self._extract_product_manufacturer(product)

        if reference_profile:
            reference_title = str(reference_profile.get("title") or query).strip()
            reference_title_normalized = self._normalize_text(reference_title)
            if reference_title_normalized and product_title_normalized == reference_title_normalized:
                return False, ["это исходный товар, а не аналог"]

            reference_brands = self._extract_brand_tokens(
                "\n".join([
                    query,
                    reference_title,
                    str(reference_profile.get("manufacturer") or ""),
                ])
            )
            product_brands = self._extract_brand_tokens(product_identity_source)
            if reference_brands and reference_brands & product_brands:
                return False, ["тот же бренд/линейка исходного товара"]

            reference_manufacturer = str(reference_profile.get("manufacturer") or "").strip()
            if (
                reference_manufacturer
                and product_manufacturer
                and self._manufacturer_matches(reference_manufacturer, product_manufacturer)
            ):
                return False, ["тот же производитель, нужен аналог другого производителя"]

        query_families = self._extract_material_families(query_source)
        product_families = self._extract_material_families(product_identity_source)
        if not product_families:
            product_families = self._extract_material_families(product_facts_source)
        if not product_families:
            product_families = self._extract_material_families(product_source)
        if query_families and not self._families_are_compatible(query_families, product_families):
            return False, ["тип материала не совпадает с ТЗ"]

        query_marks = self._extract_marks(query_source)
        product_marks = self._extract_marks(product_facts_source)
        if query_marks and not (query_marks & product_marks):
            return False, ["марка материала не совпадает"]

        query_role = self._extract_roll_role(query_source)
        product_role = self._extract_roll_role(product_facts_source)
        if query_role and product_role and query_role != product_role:
            return False, ["назначение рулонного материала не совпадает"]

        query_bases = self._extract_base_types(query_source)
        product_bases = self._extract_base_types(product_facts_source)
        if query_bases and product_bases and not (query_bases & product_bases):
            return False, ["тип основы материала не совпадает"]

        query_insulation_subtypes = self._extract_insulation_subtypes(query_source)
        product_insulation_subtypes = self._extract_insulation_subtypes(product_facts_source)
        if not product_insulation_subtypes:
            product_insulation_subtypes = self._extract_insulation_subtypes(product_source)
        if query_insulation_subtypes and not self._insulation_subtypes_are_compatible(
            query_insulation_subtypes,
            product_insulation_subtypes,
        ):
            return False, ["тип утеплителя не совпадает с ТЗ"]

        query_surface_features = self._extract_surface_features(query_source)
        product_surface_features = self._extract_surface_features(product_facts_source)
        if not product_surface_features:
            product_surface_features = self._extract_surface_features(product_source)
        if query_surface_features and not self._surface_features_are_compatible(
            query_surface_features,
            product_surface_features,
        ):
            return False, ["покрытие/облицовка материала не совпадает с ТЗ"]

        query_methods = self._extract_application_methods(query_source)
        product_methods = self._extract_application_methods(product_facts_source)
        if query_methods and product_methods and not (query_methods & product_methods):
            return False, ["способ применения материала не совпадает"]

        reasons = []
        constraints = self._extract_requirement_constraints(query, requirements)
        product_metrics = self._extract_product_metrics(product)
        for metric_name, constraint in constraints.items():
            passed, reason = self._passes_numeric_constraint(
                metric_name,
                constraint,
                product_metrics.get(metric_name),
            )
            if not passed:
                return False, [reason or "числовое требование не выполнено"]
            if reason and reason not in reasons:
                reasons.append(reason)

        if reference_profile:
            reference_price = self._parse_price_value(reference_profile.get("price"))
            product_price = self._parse_price_value(product.get("price"))
            strict_reference_price = bool(
                reference_profile.get("strict_price", reference_price is not None)
            )
            if (
                strict_reference_price
                and reference_price is not None
                and product_price is not None
                and product_price > reference_price + 0.01
            ):
                return False, ["цена аналога выше цены исходного товара"]

            reference_metrics = reference_profile.get("metrics") or {}
            reference_tolerances = {
                "thickness": 0.5,
                "mass": 0.7,
                "flex": 4.0,
            }
            for metric_name, reference_value in reference_metrics.items():
                if reference_value is None:
                    continue
                if constraints.get(metric_name) is not None:
                    continue
                product_value = product_metrics.get(metric_name)
                if product_value is None:
                    reason = (
                        f"параметр '{self._metric_label(metric_name)}' "
                        f"не подтвержден относительно исходного товара"
                    )
                    if reason not in reasons:
                        reasons.append(reason)
                    continue
                if metric_name != "flex":
                    continue
                if float(product_value) > float(reference_value) + reference_tolerances.get(metric_name, 0.0):
                    return False, [f"параметр '{self._metric_label(metric_name)}' хуже исходного товара"]

        if query_families and product_families:
            reasons.append("тип материала подтвержден")
        if query_marks and product_marks and query_marks & product_marks:
            reasons.append("марка материала подтверждена")
        if query_bases and product_bases and query_bases & product_bases:
            reasons.append("тип основы совпадает")
        if reference_profile:
            reference_price = self._parse_price_value(reference_profile.get("price"))
            product_price = self._parse_price_value(product.get("price"))
            strict_reference_price = bool(
                reference_profile.get("strict_price", reference_price is not None)
            )
            if (
                reference_price is not None
                and product_price is not None
                and product_price <= reference_price + 0.01
            ):
                if strict_reference_price:
                    reasons.append("цена не выше исходного товара")
                else:
                    reasons.append("цена не выше ориентировочной цены исходного товара")

        return True, reasons

    def _min_required_score(self, query: str, requirements: str = "") -> int:
        score = 28
        if self._extract_material_families(f"{query}\n{requirements}"):
            score += 8
        if self._extract_marks(query):
            score += 10
        if any(self._extract_requirement_constraints(query, requirements).values()):
            score += 10
        return min(score, 55)

    def _parse_ai_json_payload(self, response_text: str) -> dict[str, Any]:
        """
        Извлекает первый валидный JSON-объект из ответа модели.

        Модель иногда возвращает корректный JSON, а затем добавляет пояснение
        или второй JSON-блок. В таком формате json.loads() на всей строке падает
        с Extra data, хотя сам полезный payload уже есть в начале ответа.
        """
        text = (response_text or "").replace("\ufeff", "").replace("\u200b", "").strip()
        if not text:
            raise json.JSONDecodeError("Empty JSON response", text, 0)

        decoder = json.JSONDecoder()
        parsed_objects: list[tuple[dict[str, Any], int, int]] = []
        last_error: Optional[json.JSONDecodeError] = None

        for idx, char in enumerate(text):
            if char != "{":
                continue

            try:
                parsed, end = decoder.raw_decode(text[idx:])
            except json.JSONDecodeError as exc:
                last_error = exc
                continue

            if not isinstance(parsed, dict):
                continue

            parsed_objects.append((parsed, idx, idx + end))

        analog_objects = [
            (parsed, start, end)
            for parsed, start, end in parsed_objects
            if isinstance(parsed.get("analogs"), list)
        ]
        if analog_objects:
            best_payload, best_start, best_end = max(
                analog_objects,
                key=lambda item: len(item[0].get("analogs") or []),
            )
            trailing = text[best_end:].strip()
            if best_start > 0 or trailing or len(analog_objects) > 1:
                logger.warning(
                    "[AnalogService] AI response contained multiple/extra JSON blocks. "
                    "Using the analogs object with the largest candidate set (%s items).",
                    len(best_payload.get("analogs") or []),
                )
            return best_payload

        if parsed_objects:
            parsed, start, end = parsed_objects[0]
            trailing = text[end:].strip()
            if start > 0 or trailing:
                logger.warning(
                    "[AnalogService] AI response contained wrapper text around JSON payload. "
                    "Using the first valid JSON object."
                )
            return parsed

        if last_error is not None:
            raise last_error

        raise json.JSONDecodeError("No JSON object found", text, 0)

    @staticmethod
    def _normalize_validation_status(status: str) -> str:
        normalized = str(status or "").strip().upper()
        if normalized in {"APPROVED", "OK", "PASS", "VALID"}:
            return "APPROVED"
        if normalized in {"UNCERTAIN", "PARTIAL", "UNSURE"}:
            return "UNCERTAIN"
        if normalized in {"REJECTED", "FAIL", "INVALID", "NO"}:
            return "REJECTED"
        return "UNKNOWN"

    @staticmethod
    def _validation_status_rank(status: str) -> int:
        normalized = str(status or "").strip().upper()
        if normalized == "APPROVED":
            return 3
        if normalized == "UNCERTAIN":
            return 2
        if normalized == "REJECTED":
            return 1
        return 0

    def _candidate_sort_key(self, item: dict[str, Any]) -> tuple[int, int, int, int, float, str]:
        validation_status_rank = self._validation_status_rank(
            str(item.get("validation_status") or "")
        )
        validation_score = int(item.get("validation_score") or 0)
        match_score = int(item.get("match_score") or 0)
        source_priority = 1 if str(item.get("source") or "") == "local_db" else 0
        price = self._parse_price_value(item.get("price"))
        cheaper_first = -price if price is not None else float("-inf")
        title = str(item.get("title") or "")
        return (
            validation_status_rank,
            validation_score,
            match_score,
            source_priority,
            cheaper_first,
            title,
        )

    @staticmethod
    def _normalize_validation_parameter_list(raw_value: Any) -> list[str]:
        if not isinstance(raw_value, list):
            return []

        normalized: list[str] = []
        for item in raw_value:
            text = str(item or "").strip()
            if text and text not in normalized:
                normalized.append(text)
        return normalized

    def _should_soften_validation_rejection(
        self,
        candidate: dict[str, Any],
        decision: dict[str, Any],
    ) -> bool:
        if str(decision.get("status") or "").upper() != "REJECTED":
            return False

        comment = self._normalize_text(str(decision.get("comment") or ""))
        if not comment:
            return False

        match_reason = self._normalize_text(str(candidate.get("match_reason") or ""))
        if "не подтвержд" not in match_reason:
            return False

        product_metrics = self._extract_product_metrics(candidate)
        metric_roots = {
            "thickness": "толщин",
            "mass": "масс",
            "flex": "гибк",
        }
        for metric_name, root in metric_roots.items():
            if root in comment and root in match_reason and product_metrics.get(metric_name) is None:
                return True

        missing_data_signals = (
            "не подтвержд",
            "недостаточно дан",
            "нет дан",
            "не указ",
            "отсутств",
            "не удалось подтверд",
            "маркировк",
            "названи",
        )
        if not any(signal in comment for signal in missing_data_signals):
            return False

        contradiction_signals = (
            "ниже требования",
            "хуже требования",
            "выше допустимого",
            "марка не совпадает",
            "тип основы отличается",
            "способ применения отличается",
        )
        if any(signal in match_reason for signal in contradiction_signals):
            return False

        return True

    def _apply_ai_validation_result(
        self,
        candidates: list[dict],
        validation_payload: dict,
    ) -> tuple[list[dict], str, bool]:
        results = validation_payload.get("results")
        if not isinstance(results, list) or not results:
            return (
                candidates,
                str(validation_payload.get("summary") or "AI validation returned no decisions"),
                False,
            )

        prepared_ids = {
            str(candidate.get("candidate_id") or "").strip()
            for candidate in candidates
            if str(candidate.get("candidate_id") or "").strip()
        }
        title_to_candidate_id = {}
        for candidate in candidates:
            normalized_title = self._normalize_title_for_dedup(str(candidate.get("title") or ""))
            candidate_id = str(candidate.get("candidate_id") or "").strip()
            if normalized_title and candidate_id and normalized_title not in title_to_candidate_id:
                title_to_candidate_id[normalized_title] = candidate_id

        decisions = {}
        for entry in results:
            if not isinstance(entry, dict):
                continue
            candidate_id = str(entry.get("candidate_id") or "").strip()
            if candidate_id not in prepared_ids:
                normalized_title = self._normalize_title_for_dedup(str(entry.get("title") or ""))
                candidate_id = title_to_candidate_id.get(normalized_title, "")
            if not candidate_id:
                continue
            decisions[candidate_id] = {
                "status": self._normalize_validation_status(entry.get("status")),
                "validation_score": int(entry.get("validation_score") or 0),
                "comment": str(entry.get("comment") or "").strip(),
                "matched_parameters": self._normalize_validation_parameter_list(
                    entry.get("matched_parameters")
                ),
                "conflicting_parameters": self._normalize_validation_parameter_list(
                    entry.get("conflicting_parameters")
                ),
                "missing_parameters": self._normalize_validation_parameter_list(
                    entry.get("missing_parameters")
                ),
            }

        if not decisions:
            return (
                candidates,
                str(validation_payload.get("summary") or "AI validation returned empty mapping"),
                False,
            )

        validated: list[dict] = []
        softened_uncertain_ids: set[str] = set()
        approved_count = 0
        uncertain_count = 0
        rejected_count = 0
        matched_decisions = 0

        for candidate in candidates:
            candidate_id = str(candidate.get("candidate_id") or "")
            decision = decisions.get(candidate_id)
            if not decision:
                continue
            matched_decisions += 1

            effective_status = decision["status"]
            effective_score = decision["validation_score"]
            if self._should_soften_validation_rejection(candidate, decision):
                effective_status = "UNCERTAIN"
                effective_score = max(effective_score, 75)
                softened_uncertain_ids.add(candidate_id)

            enriched = {
                **candidate,
                "validation_status": effective_status,
                "validation_score": effective_score,
                "validation_comment": decision["comment"],
                "validation_matched_parameters": decision["matched_parameters"],
                "validation_conflicting_parameters": decision["conflicting_parameters"],
                "validation_missing_parameters": decision["missing_parameters"],
            }

            if effective_status == "APPROVED":
                approved_count += 1
            elif effective_status == "UNCERTAIN":
                uncertain_count += 1
            else:
                rejected_count += 1
            validated.append(enriched)

        if not validated:
            return (
                candidates,
                str(validation_payload.get("summary") or "AI validation returned no decisions"),
                False,
            )

        validated.sort(key=self._candidate_sort_key, reverse=True)

        summary = str(validation_payload.get("summary") or "").strip()
        if not matched_decisions:
            return (
                candidates,
                summary or "AI validation did not return decisions for current candidates",
                False,
            )

        summary_parts = [
            part for part in [
                f"Подтверждено: {approved_count}",
                f"Требуют проверки: {uncertain_count}" if uncertain_count else "",
                f"Отклонено кандидатов: {rejected_count}" if rejected_count else "",
            ]
            if part
        ]
        counts_summary = ", ".join(summary_parts)
        if summary and counts_summary:
            summary = f"{summary} {counts_summary}."
        elif counts_summary:
            summary = counts_summary

        return validated, summary, True

    async def ai_validate_candidates(
        self,
        *,
        query: str,
        requirements: str = None,
        candidates: list[dict],
    ) -> tuple[list[dict], str, str]:
        if not candidates:
            return [], "", "skipped"

        if not self.ai_service or not hasattr(self.ai_service, "validate_analog_candidates"):
            return candidates, "", "skipped"

        if self._is_ai_temporarily_blocked("default"):
            remaining = self._remaining_ai_block_seconds("default")
            logger.info(
                "[AnalogService] AI validation skipped for %s more sec (quota exhausted).",
                remaining,
            )
            return candidates, "QUOTA_EXHAUSTED", "fallback"

        prepared_candidates = []
        for idx, candidate in enumerate(candidates):
            candidate_id = str(candidate.get("candidate_id") or candidate.get("id") or f"candidate_{idx}")
            prepared_candidates.append({
                **candidate,
                "candidate_id": candidate_id,
            })

        try:
            validation_payload = await asyncio.to_thread(
                self.ai_service.validate_analog_candidates,
                query=query,
                requirements=requirements or "",
                candidates=prepared_candidates,
            )
        except Exception as e:
            error_str = str(e)
            if (
                "429" in error_str
                or "QUOTA_EXHAUSTED" in error_str
                or "SERVICE_UNAVAILABLE" in error_str
            ):
                retry_after = self._extract_retry_after_seconds(error_str)
                self._block_ai_temporarily(
                    seconds=retry_after + 1.0 if retry_after else 10.0,
                    reason="validation_failed",
                    scope="default",
                )
            logger.warning(f"[AnalogService] AI validation failed: {e}")
            return prepared_candidates, str(e), "fallback"

        validated, summary, validation_applied = self._apply_ai_validation_result(
            prepared_candidates,
            validation_payload or {},
        )
        if not validation_applied:
            logger.warning(
                "[AnalogService] AI validation payload is unusable. Falling back to prevalidated shortlist."
            )
            return prepared_candidates, summary or "AI validation returned unusable payload", "fallback"

        return validated, summary, "applied"

    def _build_search_keywords(self, query: str, requirements: str = "") -> list[str]:
        stop_words = {
            "не", "и", "или", "для", "на", "в", "с", "по", "из",
            "указано", "штука", "штук", "шт", "кг", "м2", "рул",
            "рулон", "упак", "литр", "единица", "ед", "марка",
            "тип", "вид", "класс", "материал", "товар", "продукт",
            "поставка", "закупка", "техническое", "задание", "описание",
            "объекта", "документация", "характеристики", "требования",
            "менее", "более", "ниже", "выше", "толщина", "масса", "гибкость",
            "стандарт", "гост", "ту", "сто", "бренд", "производитель",
            "линейка", "размер", "состав", "основа", "покрытие",
            "рамках", "гоз", "доп", "маркировка", "основы", "защитного",
            "покрытия", "теплостойкость", "рулона", "ширина", "группа",
            "битумно-полимерный", "битумно", "полимерный", "основа",
        }

        keywords: list[str] = []
        family_numeric_markers: set[str] = set()

        def add_keyword(raw_keyword: str) -> None:
            keyword = raw_keyword.strip().lower().strip(":;,.")
            if not keyword:
                return
            if keyword in stop_words:
                return
            if keyword in ("*", "/", "|", "(", ")", "-"):
                return
            if keyword not in family_numeric_markers and self._is_noise_token(keyword, allow_dimensions=True):
                return
            if keyword not in keywords:
                keywords.append(keyword)

        normalized_query = self._normalize_text(query)
        for raw_keyword in re.split(r"[\s,;/\(\)\n\r]+", normalized_query):
            raw_keyword = raw_keyword.strip()
            if not raw_keyword:
                continue
            add_keyword(raw_keyword)
            for part in [piece for piece in re.split(r"[-/]", raw_keyword) if piece]:
                add_keyword(part)

        source = f"{query}\n{requirements}"
        family_numeric_markers = set(self._extract_family_numeric_markers(source))
        family_tokens_map = {
            "bitumen": ["битум", "брикет"],
            "geomembrane": ["геомембрана", "профилированная", "дренажная"],
            "geotextile": ["геотекстиль", "полиэфирный", "иглопробивной"],
            "shponka": ["гидрошпонка", "шпонка", "гидропрокладка"],
        }
        for family in self._extract_material_families(source):
            for token in family_tokens_map.get(family, []):
                add_keyword(token)
        for mark in sorted(self._extract_marks(source)):
            add_keyword(mark)

        for grade in sorted(self._extract_grade_tokens(query)):
            add_keyword(grade)

        base_tokens_map = {
            "polyester": ["полиэстер"],
            "fiberglass_tissue": ["стеклоткань"],
            "fiberglass_mat": ["стеклохолст"],
            "cardboard": ["картон"],
        }
        for base in self._extract_base_types(source):
            for token in base_tokens_map.get(base, []):
                add_keyword(token)

        method_tokens_map = {
            "torch_applied": ["наплавляемый"],
            "self_adhesive": ["самоклеящийся"],
            "cold_applied": ["холодного", "нанесения"],
        }
        for method in self._extract_application_methods(source):
            for token in method_tokens_map.get(method, []):
                add_keyword(token)
        for token in self._extract_family_numeric_markers(source):
            add_keyword(token)

        role = self._extract_roll_role(source)
        if role == "top":
            add_keyword("верхний")
            add_keyword("кровельный")
        elif role == "underlay":
            add_keyword("подкладочный")

        for token in self._tokenize(query):
            if token in {"пленка", "сланец", "посыпка", "фольга"}:
                add_keyword(token)

        technical_keywords = [
            keyword for keyword in keywords
            if keyword in family_numeric_markers or not self._is_reference_product_token(keyword)
        ]
        if len(technical_keywords) >= 3:
            keywords = technical_keywords
        elif technical_keywords:
            keywords = technical_keywords + [
                keyword for keyword in keywords
                if keyword not in technical_keywords
            ]

        return keywords[:8]

    def _extract_first_number_after_label(self, text: str, patterns: list[str]) -> Optional[float]:
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                raw = match.group(1).replace(",", ".")
                try:
                    return float(raw)
                except Exception:
                    pass
        return None

    def _extract_requirements_numeric(self, query: str, requirements: str = "") -> dict:
        source = f"{query}\n{requirements}".replace(",", ".")
        return {
            "thickness": self._extract_first_number_after_label(source, [
                r"толщин[аы]?[^\d]{0,15}(\d+(?:\.\d+)?)",
                r"\b(\d+(?:\.\d+)?)\s*мм\b",
            ]),
            "mass": self._extract_first_number_after_label(source, [
                r"масс[аы]?[^\d]{0,15}(\d+(?:\.\d+)?)",
                r"\b(\d+(?:\.\d+)?)\s*кг/?м2\b",
                r"\b(\d+(?:\.\d+)?)\s*кг/?м²\b",
            ]),
            "flex": self._extract_first_number_after_label(source, [
                r"гибкост[ьи][^\-\d]{0,15}(-?\d+(?:\.\d+)?)",
                r"\b(-?\d+(?:\.\d+)?)\s*°?c\b",
            ]),
        }

    def _score_product(
        self,
        product: dict,
        query: str,
        requirements: str = "",
        reference_profile: Optional[dict[str, Any]] = None,
    ) -> tuple[int, list[str]]:
        title = self._normalize_text(product.get("title", ""))
        category = self._normalize_text(product.get("category", ""))
        material_type = self._normalize_text(product.get("material_type", ""))
        material_group = self._normalize_text(product.get("material_group", ""))
        product_family_name = self._normalize_text(product.get("product_family", ""))
        base_material = self._normalize_text(product.get("base_material", ""))
        description = self._normalize_text(product.get("description", ""))
        specs_text = self._normalize_text(self._flatten_specs(product.get("specs") or {}))
        indexed_search_text = self._normalize_text(product.get("search_text", ""))

        haystack_title = f"{title} {category} {material_type}".strip()
        haystack_full = (
            f"{title} {category} {material_type} {material_group} {product_family_name} "
            f"{base_material} {description} {specs_text} {indexed_search_text}"
        ).strip()
        normalized_query = self._normalize_text(query)

        all_tokens = self._build_scoring_tokens(query, requirements)

        score = 0
        reasons = []

        title_hits = self._match_keywords_in_text(all_tokens, haystack_title)
        full_hits = self._match_keywords_in_text(all_tokens, haystack_full)

        if title_hits:
            score += min(40, len(title_hits) * 10)
            reasons.append(f"совпадения в названии: {', '.join(title_hits[:5])}")

        if normalized_query and normalized_query in haystack_full:
            score += 14
            reasons.append("запрос почти полностью совпадает с карточкой")
        elif len(title_hits) >= 2:
            score += 10
            reasons.append("несколько ключевых слов совпали в карточке")

        extra_full_hits = [token for token in full_hits if token not in title_hits]
        if extra_full_hits:
            score += min(20, len(extra_full_hits) * 4)
            reasons.append(f"совпадения в описании/характеристиках: {', '.join(extra_full_hits[:5])}")

        query_families = self._extract_material_families(f"{query}\n{requirements}")
        product_families = self._extract_material_families(haystack_title)
        if not product_families:
            product_families = self._extract_material_families(haystack_full)
        if query_families and self._families_are_compatible(query_families, product_families):
            score += 22
            reasons.append("совпадает тип материала")

        query_family_tokens = [
            token
            for token in self._build_search_keywords(query, requirements)
            if len(token) >= 4 and not self._is_noise_token(token, allow_dimensions=True)
        ]
        family_hits = self._match_keywords_in_text(
            query_family_tokens,
            f"{material_group} {product_family_name} {base_material}",
        )
        if family_hits:
            score += min(18, len(family_hits) * 6)
            reasons.append(f"совпадает семейство/линейка: {', '.join(family_hits[:3])}")

        query_insulation_subtypes = self._extract_insulation_subtypes(f"{query}\n{requirements}")
        product_insulation_subtypes = self._extract_insulation_subtypes(haystack_full)
        if query_insulation_subtypes:
            if self._insulation_subtypes_are_compatible(query_insulation_subtypes, product_insulation_subtypes):
                score += 18
                reasons.append("совпадает тип утеплителя")
            elif product_insulation_subtypes:
                score -= 18
                reasons.append("тип утеплителя отличается")
            else:
                score -= 12
                reasons.append("тип утеплителя не подтвержден")

        query_surface_features = self._extract_surface_features(f"{query}\n{requirements}")
        product_surface_features = self._extract_surface_features(haystack_full)
        if query_surface_features:
            if self._surface_features_are_compatible(query_surface_features, product_surface_features):
                score += 10
                reasons.append("совпадает покрытие/облицовка")
            elif product_surface_features:
                score -= 12
                reasons.append("покрытие/облицовка отличается")
            else:
                score -= 8
                reasons.append("покрытие/облицовка не подтверждены")

        query_marks = self._extract_marks(query)
        product_marks = self._extract_marks(haystack_full)
        if query_marks:
            if query_marks & product_marks:
                score += 25
                reasons.append(f"совпадает марка: {', '.join(sorted(query_marks & product_marks))}")
            else:
                score -= 20
                reasons.append("марка не совпадает")

        query_bases = self._extract_base_types(f"{query}\n{requirements}")
        product_bases = self._extract_base_types(haystack_full)
        if query_bases and product_bases:
            if query_bases & product_bases:
                score += 12
                reasons.append("совпадает тип основы")
            else:
                score -= 15
                reasons.append("тип основы отличается")

        query_methods = self._extract_application_methods(f"{query}\n{requirements}")
        product_methods = self._extract_application_methods(haystack_full)
        if query_methods and product_methods:
            if query_methods & product_methods:
                score += 10
                reasons.append("совпадает способ применения")
            else:
                score -= 12
                reasons.append("способ применения отличается")

        query_grades = self._extract_grade_tokens(query)
        product_grades = self._extract_grade_tokens(haystack_full)
        if query_grades and product_grades:
            if query_grades & product_grades:
                score += 8
                reasons.append("совпадает класс материала")
            elif "премиум" in product_grades and "премиум" not in query_grades:
                score -= 8
                reasons.append("класс материала выше исходного товара")

        constraints = self._extract_requirement_constraints(query, requirements)
        numeric_req = self._extract_requirements_numeric(query, requirements)
        product_metrics = self._extract_product_metrics(product)
        product_thickness = product_metrics["thickness"]
        product_mass = product_metrics["mass"]
        product_flex = product_metrics["flex"]

        requirement_tolerances = {
            "thickness": 0.35,
            "mass": 0.5,
            "flex": 5.0,
        }
        for metric_name, constraint in constraints.items():
            if constraint is None:
                continue

            product_value = product_metrics.get(metric_name)
            label = self._metric_label(metric_name)
            if product_value is None:
                score -= 12 if metric_name != "mass" else 10
                reasons.append(f"{label} не подтверждена")
                continue

            target = float(constraint["value"])
            tolerance = requirement_tolerances.get(metric_name, 0.0)
            mode = constraint["mode"]

            if metric_name == "flex":
                if mode == "max":
                    if product_value <= target + tolerance:
                        delta = abs(product_value - target)
                        if delta <= 2:
                            score += 12
                            reasons.append("гибкость соответствует требованию")
                        elif delta <= 6:
                            score += 7
                            reasons.append("гибкость близка к требованию")
                        else:
                            score += 4
                            reasons.append("гибкость лучше требования")
                    else:
                        score -= 8
                        reasons.append("гибкость хуже требования")
                elif mode == "min":
                    if product_value >= target - tolerance:
                        score += 10
                        reasons.append("гибкость соответствует требованию")
                    else:
                        score -= 8
                        reasons.append("гибкость ниже требования")
                else:
                    delta = abs(product_value - target)
                    if delta <= 3:
                        score += 12
                        reasons.append("гибкость близка к требованию")
                    elif delta <= 8:
                        score += 5
                        reasons.append("гибкость похожа на требование")
                    else:
                        score -= 6
                        reasons.append("гибкость отличается")
                continue

            if mode == "min":
                if product_value + tolerance < target:
                    score -= 8
                    reasons.append(f"{label} ниже требования")
                else:
                    excess = product_value - target
                    if excess <= tolerance:
                        score += 14
                        reasons.append(f"{label} соответствует минимальному требованию")
                    elif excess <= tolerance * 2.5:
                        score += 9
                        reasons.append(f"{label} близка к требованию")
                    else:
                        score += 4
                        reasons.append(f"{label} выше минимального требования")
            elif mode == "max":
                if product_value - tolerance > target:
                    score -= 8
                    reasons.append(f"{label} выше допустимого")
                else:
                    delta = target - product_value
                    if delta <= tolerance:
                        score += 12
                        reasons.append(f"{label} соответствует требованию")
                    elif delta <= tolerance * 2.5:
                        score += 7
                        reasons.append(f"{label} близка к требованию")
                    else:
                        score += 4
                        reasons.append(f"{label} ниже предельного значения")
            else:
                delta = abs(product_value - target)
                if delta <= tolerance:
                    score += 14
                    reasons.append(f"{label} близка к требованию")
                elif delta <= tolerance * 2.5:
                    score += 7
                    reasons.append(f"{label} похожа на требование")
                else:
                    score -= 5
                    reasons.append(f"{label} отличается")

        if reference_profile:
            reference_price = self._parse_price_value(reference_profile.get("price"))
            product_price = self._parse_price_value(product.get("price"))
            strict_reference_price = bool(
                reference_profile.get("strict_price", reference_price is not None)
            )
            if reference_price is not None and product_price is not None:
                if product_price <= reference_price + 0.01:
                    if strict_reference_price:
                        score += 10
                        reasons.append("цена не выше исходного товара")
                    else:
                        saving_ratio = (
                            (reference_price - product_price) / reference_price
                            if reference_price > 0
                            else 0.0
                        )
                        if saving_ratio >= 0.12:
                            score += 14
                            reasons.append("цена заметно ниже ориентировочной цены исходного товара")
                        elif saving_ratio >= 0.04:
                            score += 12
                            reasons.append("цена ниже ориентировочной цены исходного товара")
                        else:
                            score += 9
                            reasons.append("цена не выше ориентировочной цены исходного товара")
                else:
                    if strict_reference_price:
                        score -= 20
                        reasons.append("цена выше исходного товара")
                    else:
                        overprice_ratio = (
                            (product_price - reference_price) / reference_price
                            if reference_price > 0
                            else 0.0
                        )
                        if overprice_ratio >= 0.2:
                            score -= 18
                            reasons.append("цена значительно выше ориентировочной цены исходного товара")
                        elif overprice_ratio >= 0.1:
                            score -= 14
                            reasons.append("цена существенно выше ориентировочной цены исходного товара")
                        else:
                            score -= 10
                            reasons.append("цена выше ориентировочной цены исходного товара")

            reference_metrics = reference_profile.get("metrics") or {}
            for metric_name, reference_value in reference_metrics.items():
                if reference_value is None or numeric_req.get(metric_name) is not None:
                    continue
                product_value = product_metrics.get(metric_name)
                if product_value is None:
                    missing_penalties = {
                        "thickness": 1,
                        "mass": 2,
                        "flex": 5,
                    }
                    score -= missing_penalties.get(metric_name, 2)
                    reasons.append(f"параметр '{self._metric_label(metric_name)}' не подтвержден")
                    continue
                if metric_name == "thickness":
                    delta = abs(float(product_value) - float(reference_value))
                    if float(product_value) + 0.25 < float(reference_value):
                        score -= 4
                        reasons.append("толщина ниже исходного товара")
                    elif delta <= 0.35:
                        score += 8
                        reasons.append("толщина близка к исходному товару")
                    elif delta <= 0.7:
                        score += 5
                        reasons.append("толщина похожа на исходный товар")
                    elif delta <= 1.0:
                        score += 2
                        reasons.append("толщина не ниже исходного товара")
                    else:
                        score -= 3
                        reasons.append("толщина заметно отличается от исходного товара")
                elif metric_name == "mass":
                    delta = abs(float(product_value) - float(reference_value))
                    if float(product_value) + 0.4 < float(reference_value):
                        score -= 4
                        reasons.append("масса ниже исходного товара")
                    elif delta <= 0.25:
                        score += 5
                        reasons.append("масса близка к исходному товару")
                    elif delta <= 0.6:
                        score += 3
                        reasons.append("масса не ниже исходного товара")
                    else:
                        score += 1
                        reasons.append("масса сопоставима с исходным товаром")
                elif metric_name == "flex":
                    delta = abs(float(product_value) - float(reference_value))
                    if float(product_value) > float(reference_value) + 4:
                        score -= 8
                        reasons.append("гибкость хуже исходного товара")
                    elif delta <= 2:
                        score += 8
                        reasons.append("гибкость близка к исходному товару")
                    elif delta <= 5:
                        score += 5
                        reasons.append("гибкость близка к исходному товару")
                    else:
                        score += 2
                        reasons.append("гибкость не хуже исходного товара")

        query_descriptors = self._extract_query_descriptors(requirements or "")
        if not query_descriptors and not requirements:
            query_descriptors = [
                token for token in self._extract_query_descriptors(query)
                if not self._is_reference_product_token(token)
            ]
        if query_descriptors:
            descriptor_hits = [token for token in query_descriptors if token in haystack_full]
            if descriptor_hits:
                score += min(12, len(descriptor_hits) * 6)
                reasons.append(f"совпадают ключевые свойства: {', '.join(descriptor_hits[:3])}")
            elif not query_marks and not query_bases and not any(numeric_req.values()):
                score -= 12
                reasons.append("не подтверждены ключевые свойства из запроса")

        if score < 0:
            score = 0

        return score, reasons

    def _normalize_title_for_dedup(self, title: str) -> str:
        if not title:
            return ""
        return re.sub(r"\s+", " ", re.sub(r"[^a-zA-Zа-яА-Я0-9]+", " ", title.lower())).strip()

    # ─────────────────────────────────────────────────────────────────────────
    # ПОИСК В ЛОКАЛЬНОЙ БД
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _clean_search_query(raw_query: str) -> str:
        """
        Очищает поисковый запрос от мусора из парсинга документов.

        Удаляет:
          - единицы измерения в скобках: (штука), (м2), (кг), (рул)
          - метки отсутствия данных: (не указано)*, / (не указано)*
          - служебные символы парсера: *, /
          - пайп-формат: "Ключ | Значение | источник.docx, стр.N |"
          - технические суффиксы: ГОСТ XXXXX, ТУ XXXXX
        """
        import re

        if not raw_query:
            return ""

        query = raw_query.strip()

        # ШАБЛОН 1: Пайп-формат "Поле | Значение | файл.docx, стр.N |"
        if "|" in query:
            parts = [p.strip() for p in query.split("|")]
            FIELD_NAMES = {
                "предмет закупки", "наименование", "описание",
                "объект закупки", "товар", "номенклатура",
                "позиция", "материал", "продукт", "изделие",
            }
            FILE_PATTERN = re.compile(
                r"\.(docx|pdf|xlsx?|doc)\b|стр\.?\s*\d+",
                re.IGNORECASE
            )
            good_parts = [
                p for p in parts
                if p
                and p.lower() not in FIELD_NAMES
                and not FILE_PATTERN.search(p)
                and len(p) > 3
            ]
            if good_parts:
                query = max(good_parts, key=len)

        # ШАБЛОН 2: Единицы измерения в скобках — (штука), (м2), (кг) и т.д.
        query = re.sub(
            r"\(\s*(штука|штук|шт|м2|м\.кв|кв\.м|м|рулон|рул|кг|г|л|литр|"
            r"упак|уп|комплект|компл|набор|пара|п\.м|пм|погонный метр)\s*\)",
            "",
            query,
            flags=re.IGNORECASE
        )

        # ШАБЛОН 3: Метки отсутствия данных
        query = re.sub(
            r"/?\s*\(\s*не\s+указано\s*\)\s*\*?",
            "",
            query,
            flags=re.IGNORECASE
        )
        query = re.sub(r"\(\s*не\s+указано\s*\)", "", query, flags=re.IGNORECASE)
        query = re.sub(r"\*", "", query)

        # ШАБЛОН 4: Слэш в конце или начале
        query = query.strip("/").strip()

        # ШАБЛОН 5: ГОСТ и ТУ с номерами
        query = re.sub(r"\s+ГОСТ\s+[\d\-]+", "", query, flags=re.IGNORECASE)
        query = re.sub(r"\s+ТУ\s+[\d\.\-]+", "", query, flags=re.IGNORECASE)

        # ШАБЛОН 6: Количество + единица в скобках "10 кг" "(20 л)"
        query = re.sub(
            r"\s*\(\s*\d+[\.,]?\d*\s*(кг|г|л|м2|м|шт|рул)\s*\)",
            "",
            query,
            flags=re.IGNORECASE
        )

        # Финальная очистка пробелов
        query = re.sub(r"\s{2,}", " ", query).strip().strip(",").strip("/").strip()

        import logging
        logger = logging.getLogger("LegalAI")
        logger.info(
            f"[AnalogService] Query cleaned: '{raw_query[:80]}' -> '{query}'"
        )
        return query if query else raw_query.strip()

    def search_local_db(
        self,
        query: str,
        category: str = None,
        limit: int = 10,
        requirements: str = None,
        reference_profile: Optional[dict[str, Any]] = None,
    ) -> list:
        """
        Поиск аналогов в локальной БД с предварительной очисткой запроса.
        Поддерживает поиск по нескольким ключевым словам одновременно.
        """
        import re
        from sqlalchemy import text
        import json as json_lib

        clean_query = self._clean_search_query(query)
        if not clean_query:
            logger.warning(f"[AnalogService] Empty query after cleaning: '{query}'")
            return []

        logger.info(
            f"[AnalogService] Local DB search: "
            f"raw='{query[:60]}' -> clean='{clean_query}' | category={category}"
        )

        keywords = self._build_search_keywords(clean_query, requirements or "")

        if not keywords:
            logger.warning(
                f"[AnalogService] No valid keywords after filtering: "
                f"raw='{query}' clean='{clean_query}'"
            )
            return []

        logger.info(
            f"[AnalogService] Search keywords: {keywords} "
            f"(from clean query: '{clean_query}')"
        )

        min_score = self._min_required_score(clean_query, requirements or "")
        results = []
        try:
            with self.db_session_factory() as session:
                candidate_ids = self._load_candidate_ids_from_fts(
                    session,
                    [clean_query, *(requirements or "").split("\n"), *keywords],
                    limit=max(limit * 120, 1200),
                )
                rows = list(
                    self._load_local_search_rows(
                        session,
                        limit=max(limit * 120, 1200) if candidate_ids else max(limit * 80, 2000),
                        candidate_ids=candidate_ids or None,
                    )
                )
                if candidate_ids and len(rows) < max(limit * 25, 250):
                    fallback_rows = self._load_local_search_rows(
                        session,
                        limit=max(limit * 80, 2000),
                        candidate_ids=None,
                    )
                    seen_ids = {row.id for row in rows}
                    for row in fallback_rows:
                        if row.id not in seen_ids:
                            rows.append(row)
                            seen_ids.add(row.id)
                normalized_category = self._normalize_text(category or "")

                for row in rows:
                    specs = {}
                    if row.specs:
                        try:
                            specs = (
                                json_lib.loads(row.specs)
                                if isinstance(row.specs, str)
                                else row.specs
                            )
                        except Exception:
                            specs = {}

                    product_payload = {
                        "id": row.id,
                        "title": row.title,
                        "category": row.category,
                        "material_type": row.material_type,
                        "price": row.price,
                        "specs": specs,
                        "url": self._normalize_external_url(row.url),
                        "description": row.description,
                        "source": "local_db",
                        "quality_score": getattr(row, "quality_score", 0),
                        "normalized_title": getattr(row, "normalized_title", None),
                        "material_group": getattr(row, "material_group", None),
                        "material_subgroup": getattr(row, "material_subgroup", None),
                        "product_family": getattr(row, "product_family", None),
                        "base_material": getattr(row, "base_material", None),
                        "search_text": getattr(row, "search_text", None),
                    }

                    if normalized_category:
                        category_text = self._normalize_text(
                            f"{product_payload.get('category', '')} "
                            f"{product_payload.get('material_type', '')}"
                        )
                        if normalized_category not in category_text:
                            continue

                    matches_keywords, matched_keywords = self._product_matches_keywords(
                        product_payload,
                        keywords,
                    )
                    if not matches_keywords:
                        continue

                    passes_filters, filter_reasons = self._passes_hard_filters(
                        product_payload,
                        clean_query,
                        requirements or "",
                        reference_profile=reference_profile,
                    )
                    if not passes_filters:
                        continue

                    score, reasons = self._score_product(
                        product_payload,
                        clean_query,
                        requirements or "",
                        reference_profile=reference_profile,
                    )
                    if score < min_score:
                        continue

                    match_reasons = []
                    if matched_keywords:
                        match_reasons.append(
                            f"совпало по поиску: {', '.join(matched_keywords[:4])}"
                        )
                    for reason in filter_reasons + reasons:
                        if reason not in match_reasons:
                            match_reasons.append(reason)

                    results.append({
                        **product_payload,
                        "match_score": self._normalize_match_score(score),
                        "match_reason": "; ".join(match_reasons[:5]),
                        "_score": score,
                    })

                results.sort(key=lambda x: x.get("_score", 0), reverse=True)
                for r in results:
                    r.pop("_score", None)
                results = results[:limit]

        except Exception as e:
            logger.error(f"[AnalogService] Local DB search error: {e}")

        logger.info(
            f"[AnalogService] Local DB: found {len(results)} results "
            f"for '{clean_query}'"
        )
        return results

    def build_preview_result(
        self,
        query: str,
        requirements: str = None,
        limit: int = 10,
    ) -> dict[str, Any]:
        """
        Быстрый предварительный результат без AI-вызовов.
        Использует только локальное восстановление референса и локальную БД,
        чтобы UI мог отрисовать черновой список до фоновой internet/AI дообработки.
        """
        clean_query = self._clean_search_query(query)
        if not clean_query:
            return {
                "query": clean_query,
                "original_query": query,
                "local_results": [],
                "ai_results": [],
                "total": 0,
                "ai_error": "",
                "validation_error": "",
                "validation_summary": "",
                "reference_profile": None,
            }

        reference_profile = self._resolve_reference_profile_local(clean_query)
        local_results = self.search_local_db(
            clean_query,
            limit=limit,
            requirements=requirements,
            reference_profile=reference_profile,
        )

        return {
            "query": clean_query,
            "original_query": query,
            "local_results": local_results,
            "ai_results": [],
            "total": len(local_results),
            "ai_error": "",
            "validation_error": "",
            "validation_summary": "",
            "reference_profile": self._public_reference_profile(reference_profile),
        }

    @staticmethod
    def _candidate_has_validation(candidate: dict[str, Any]) -> bool:
        if not isinstance(candidate, dict):
            return False
        return any(
            key in candidate
            for key in [
                "validation_status",
                "validation_score",
                "validation_comment",
                "validation_matched_parameters",
                "validation_conflicting_parameters",
                "validation_missing_parameters",
            ]
        )

    def _dedupe_ai_results(
        self,
        local_results: list[dict[str, Any]],
        ai_results: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        seen_titles = {
            self._normalize_title_for_dedup(r.get("title", ""))
            for r in local_results
            if r.get("title")
        }
        ai_unique: list[dict[str, Any]] = []
        for candidate in ai_results:
            normalized_title = self._normalize_title_for_dedup(candidate.get("title", ""))
            if not normalized_title or normalized_title in seen_titles:
                continue
            ai_unique.append(candidate)
            seen_titles.add(normalized_title)
        return ai_unique

    async def refine_search_result(
        self,
        *,
        base_result: dict[str, Any],
        query: str,
        requirements: str = None,
        use_ai: bool = True,
        limit: int = 10,
        retry_internet_only: bool = False,
    ) -> dict[str, Any]:
        """
        Дообрабатывает уже собранный preview/result без повторного локального поиска.

        При retry_internet_only=True повторяет только grounded internet-search и
        не запускает AI validation заново, чтобы не тратить default-квоту на
        каждом цикле повтора.
        """
        clean_query = self._clean_search_query(query)
        payload = copy.deepcopy(base_result or {})
        local_results = list(payload.get("local_results") or [])
        existing_ai_results = list(payload.get("ai_results") or [])
        reference_profile = payload.get("reference_profile")

        effective_requirements = self._augment_requirements_with_reference(
            requirements,
            reference_profile,
        )

        ai_results = existing_ai_results
        ai_error = ""
        if use_ai:
            fresh_ai_results, ai_error = await self.search_ai(
                query=clean_query,
                requirements=effective_requirements,
                max_results=max(1, min(limit, 5)),
                local_db_products=local_results,
                reference_profile=reference_profile,
            )
            ai_results = self._dedupe_ai_results(local_results, fresh_ai_results)

        validation_error = ""
        validation_summary = str(payload.get("validation_summary") or "")
        if not retry_internet_only and use_ai:
            combined_candidates = [
                {**item, "source": item.get("source") or "local_db"}
                for item in (local_results + ai_results)
            ]
            if combined_candidates:
                validated_candidates, validation_summary, validation_state = await self.ai_validate_candidates(
                    query=clean_query,
                    requirements=effective_requirements,
                    candidates=combined_candidates,
                )
                if validation_state == "applied":
                    local_results = [item for item in validated_candidates if item.get("source") == "local_db"]
                    ai_results = [item for item in validated_candidates if item.get("source") == "ai_search"]
                    local_results.sort(key=self._candidate_sort_key, reverse=True)
                    ai_results.sort(key=self._candidate_sort_key, reverse=True)
                    if not validated_candidates:
                        validation_error = validation_summary or "AI validation rejected all candidates"
                elif validation_state == "fallback":
                    validation_summary = validation_summary or "AI validation unavailable"
                    if not combined_candidates:
                        validation_error = validation_summary
        else:
            local_results.sort(key=self._candidate_sort_key, reverse=True)
            ai_results.sort(key=self._candidate_sort_key, reverse=True)

        return {
            "query": clean_query,
            "original_query": query,
            "local_results": local_results,
            "ai_results": ai_results,
            "total": len(local_results) + len(ai_results),
            "ai_error": ai_error,
            "validation_error": validation_error,
            "validation_summary": validation_summary,
            "reference_profile": self._public_reference_profile(reference_profile),
        }

    # ─────────────────────────────────────────────────────────────────────────
    # ПОИСК ЧЕРЕЗ GEMINI AI + GOOGLE SEARCH
    # ─────────────────────────────────────────────────────────────────────────

    async def search_ai(
        self,
        query: str,
        requirements: str = None,
        max_results: int = 5,
        local_db_products: list = None,
        reference_profile: Optional[dict[str, Any]] = None,
    ) -> tuple[list, str]:
        effective_requirements = requirements or ""
        if self._is_query_ai_temporarily_blocked(
            query,
            effective_requirements,
            scope="grounded_search",
        ):
            remaining = self._remaining_query_ai_block_seconds(
                query,
                effective_requirements,
                scope="grounded_search",
            )
            logger.info(
                f"[AnalogService] AI query cooldown active for {remaining} more sec "
                f"(quota exhausted). Skipping AI search for query='{query[:120]}'."
            )
            return [], "QUOTA_EXHAUSTED"

        # Проверяем grounded cooldown отдельно от reference/default cooldown.
        if self._is_ai_temporarily_blocked("grounded_search"):
            remaining = self._remaining_ai_block_seconds("grounded_search")
            logger.info(
                f"[AnalogService] AI is blocked for {remaining} more sec "
                f"(quota exhausted). Skipping AI search."
            )
            return [], "QUOTA_EXHAUSTED"

        logger.info(f"[AnalogService] AI search: '{query}' | requirements={bool(requirements)}")

        if not self.ai_service:
            logger.error(f"[AnalogService] AI service is not initialized")
            return [], "AI service is not initialized"

        # Очищаем запрос перед отправкой в AI
        clean_q = self._clean_search_query(query)

        # Формируем список товаров из локальной БД для AI
        local_items_text = ""
        if local_db_products:
            lines = []
            for p in local_db_products[:10]:
                specs_str = ", ".join(
                    f"{k}: {v}"
                    for k, v in (p.get("specs") or {}).items()
                    if k and v
                )[:200]
                lines.append(
                    f"- {p['title']} | {p.get('category','')} | "
                    f"Цена: {p.get('price','н/д')} руб | {specs_str}"
                )
            local_items_text = "\n".join(lines)

        reference_text = ""
        if reference_profile:
            ref_lines = []
            if reference_profile.get("title"):
                ref_lines.append(f"Исходный товар: {reference_profile['title']}")
            if reference_profile.get("manufacturer"):
                ref_lines.append(f"Производитель исходного товара: {reference_profile['manufacturer']}")
            if reference_profile.get("price") is not None:
                if reference_profile.get("strict_price", reference_profile.get("price") is not None):
                    ref_lines.append(
                        f"Цена исходного товара подтверждена: {reference_profile['price']} "
                        f"{reference_profile.get('price_unit') or 'руб'}"
                    )
                else:
                    ref_lines.append(
                        f"Цена исходного товара ориентировочная: {reference_profile['price']} "
                        f"{reference_profile.get('price_unit') or 'руб'}"
                    )
                    ref_lines.append(
                        "Ориентировочную цену использовать только как слабый ориентир, не как жесткий потолок."
                    )
            ref_specs = reference_profile.get("specs") or {}
            for key, value in list(ref_specs.items())[:8]:
                ref_lines.append(f"{key}: {value}")
            if ref_lines:
                reference_text = "\n".join(ref_lines)

        query_profile_text = self._build_query_profile_summary_text(
            clean_q,
            effective_requirements,
        )

        prompt = f"""Ты эксперт по материально-техническому снабжению и подбору аналогов промышленной продукции для тендерных закупок.

ЗАДАЧА: Подобрать аналоги для товара/материала: "{clean_q}"

{"ТЕХНИЧЕСКИЕ ТРЕБОВАНИЯ ИЗ ТЗ ТЕНДЕРА:" + chr(10) + requirements[:800] if requirements else ""}

{"РЕФЕРЕНСНЫЕ ХАРАКТЕРИСТИКИ ИСХОДНОГО ТОВАРА:" + chr(10) + reference_text[:1200] if reference_text else ""}

{"НОРМАЛИЗОВАННЫЙ ТЕХПРОФИЛЬ ЗАПРОСА:" + chr(10) + query_profile_text[:1000] if query_profile_text else ""}

{"УЖЕ ЕСТЬ В НАШЕМ КАТАЛОГЕ:" + chr(10) + local_items_text if local_items_text else ""}

ИНСТРУКЦИЯ:
1. Используй исходное название, бренд, серию и артикул ТОЛЬКО как референс для восстановления точных характеристик исходного товара.
2. Если локальная карточка исходного товара неполная, используй нормализованный техпрофиль запроса как основные поисковые критерии.
3. Не требуй совпадения бренда, серии, артикула или коммерческого названия у аналога, если это не указано прямо в ТЗ.
4. Подбирай ТОЛЬКО материалы того же типа, что и исходный товар. Не подменяй рулонный материал мастикой, мембрану праймером и т.д.
5. Если в запросе или ТЗ есть явная марка/основа/тип слоя (например ЭПП, ТКП, стеклоткань, полиэстер, верхний/нижний слой) — аналог обязан это соблюдать.
6. Если в ТЗ есть числовые требования (толщина, масса, гибкость и т.д.), НЕ включай товар, если параметр ниже/хуже требования.
7. Если цена исходного товара подтверждена точно, аналог не должен быть дороже исходного товара. Если цена помечена как ориентировочная, используй ее только как мягкое предпочтение и НЕ отсеивай кандидата только из-за более высокой цены.
8. Если часть параметров на странице товара не указана, это НЕ запрет на включение кандидата. Разрешено предлагать товар, если совпадает большинство ключевых требований и нет подтвержденных противоречий. Неподтвержденные параметры явно помечай как "не подтверждено".
9. Предпочитай аналоги других производителей. Совпадение по производителю допустимо только если иначе невозможно восстановить профиль товара и нет альтернатив.
10. Если в нашем каталоге уже есть подходящий аналог другого производителя — укажи его первым со score >= 85.
11. Найди ещё {max_results} строгих аналогов от других производителей.
12. НЕ ВЫДУМЫВАЙ товары, производителей, характеристики, артикулы и URL. Возвращай только те товары, существование которых подтверждается конкретным найденным интернет-источником из Google grounding.
13. Для каждого аналога укажи конкретные технические характеристики и в поле match_reason перечисли, какие именно требования совпали, а какие параметры остались неподтвержденными.
14. Для каждого аналога ОБЯЗАТЕЛЬНО укажи source_title и source_url — это точный заголовок и URL интернет-источника, по которому подтверждено существование этого товара. Если подтвержденного источника нет, НЕ включай такой товар в ответ вообще.
15. В поле url указывай только прямую абсолютную ссылку на карточку товара, начинающуюся с https://. Не возвращай ссылки на поиск, главную страницу, категорию, редиректы Google/Yandex или markdown-обертки. Если прямой карточки нет, верни пустую строку, но source_title/source_url все равно должны быть заполнены.

ВАЖНО: Верни ТОЛЬКО JSON без пояснений и markdown-блоков.

Формат ответа:
{{
  "analogs": [
    {{
      "title": "Название аналога",
      "manufacturer": "Производитель",
      "material_type": "Тип/Категория товара",
      "specs": {{
        "Характеристика 1": "Значение 1",
        "Характеристика 2": "Значение 2"
      }},
      "price": 100,
      "price_unit": "руб/ед",
      "source_title": "Заголовок источника, где найден товар",
      "source_url": "Подтверждающий источник из интернета",
      "url": "ссылка на товар",
      "match_reason": "Почему это аналог",
      "match_score": 90,
      "in_local_db": true
    }}
  ]
}}"""

        try:
            try:
                response_payload = await asyncio.to_thread(
                    lambda: self.ai_service.generate_with_search(prompt, include_sources=True)
                )
            except TypeError:
                response_payload = await asyncio.to_thread(
                    self.ai_service.generate_with_search,
                    prompt,
                )
            if isinstance(response_payload, dict):
                response_text = str(response_payload.get("text") or "")
                grounding_sources = response_payload.get("sources") or []
            else:
                response_text = str(response_payload or "")
                grounding_sources = []
            if not response_text:
                logger.warning(
                    f"[AnalogService] AI returned empty response."
                )
                return [], "Empty response"
        except Exception as e:
            error_str = str(e)
            is_quota = "429" in error_str or "RESOURCE_EXHAUSTED" in error_str or "QUOTA_EXHAUSTED" in error_str
            is_unavailable = "недоступен" in error_str or "временно" in error_str or "SERVICE_UNAVAILABLE" in error_str
            is_empty_response = (
                "Empty response text from Gemini" in error_str
                or "Empty response object from Gemini" in error_str
            )

            if is_quota or is_unavailable or is_empty_response:
                logger.warning(
                    f"[AnalogService] AI quota/unavailable: {error_str[:140]}. "
                    f"AI search skipped — returning local DB results only."
                )
                retry_after = self._extract_retry_after_seconds(error_str)
                self._block_ai_temporarily(
                    seconds=retry_after + 1.0 if retry_after else 10.0,
                    reason="search_ai_failed",
                    scope="grounded_search",
                )
                if is_quota:
                    self._block_query_ai_temporarily(
                        query=clean_q,
                        requirements=effective_requirements,
                        seconds=retry_after + 1.0 if retry_after else self._grounded_query_quota_cooldown_seconds,
                        reason="search_ai_quota_exhausted",
                        scope="grounded_search",
                    )
                return [], "QUOTA_EXHAUSTED" if is_quota else "SERVICE_UNAVAILABLE"
            else:
                logger.error(f"[AnalogService] AI search error: {e}")
                return [], str(e)

        try:
            data = self._parse_ai_json_payload(response_text)
            self._clear_query_ai_block(
                clean_q,
                effective_requirements,
                scope="grounded_search",
            )
            analogs = data.get("analogs", [])
            min_score = self._min_required_score(clean_q, requirements or "")

            result = []
            used_grounding_urls: set[str] = set()
            for a in analogs[:max_results]:
                preferred_source_title = str(a.get("source_title") or "").strip()
                preferred_source_url = str(a.get("source_url") or "").strip()
                ranked_grounding_sources = self._rank_grounding_sources(
                    candidate_title=a.get("title", ""),
                    candidate_manufacturer=a.get("manufacturer", ""),
                    grounding_sources=grounding_sources,
                    preferred_source_title=preferred_source_title,
                    preferred_source_url=preferred_source_url,
                )
                confirmed_source = self._select_confirmed_source(
                    candidate_title=a.get("title", ""),
                    candidate_manufacturer=a.get("manufacturer", ""),
                    grounding_sources=grounding_sources,
                    preferred_source_title=preferred_source_title,
                    preferred_source_url=preferred_source_url,
                )
                source_evidence_available = bool(
                    ranked_grounding_sources or preferred_source_title or preferred_source_url
                )
                if not source_evidence_available:
                    direct_validated_source_url = ""
                    if preferred_source_url:
                        direct_validated_source_url = self._validate_external_product_url(
                            preferred_source_url,
                            title=a.get("title", ""),
                            manufacturer=a.get("manufacturer", ""),
                        )
                    if not direct_validated_source_url and a.get("url"):
                        direct_validated_source_url = self._validate_external_product_url(
                            a.get("url", ""),
                            title=a.get("title", ""),
                            manufacturer=a.get("manufacturer", ""),
                        )
                    if direct_validated_source_url:
                        confirmed_source = {
                            "title": preferred_source_title or str(a.get("title") or ""),
                            "url": direct_validated_source_url,
                        }
                        source_evidence_available = True

                if not source_evidence_available:
                    logger.warning(
                        "[AnalogService] Discarded AI analog without matching grounding source: %s",
                        a.get("title", ""),
                    )
                    continue
                raw_url = a.get("url", "")
                normalized_url = self._select_grounding_source_url(
                    candidate_title=a.get("title", ""),
                    candidate_manufacturer=a.get("manufacturer", ""),
                    grounding_sources=grounding_sources,
                    preferred_source_title=preferred_source_title,
                    preferred_source_url=preferred_source_url,
                    used_urls=used_grounding_urls,
                )
                if normalized_url and confirmed_source.get("url") and normalized_url == str(confirmed_source.get("url") or ""):
                    resolved_better_url = self._resolve_product_url_from_source_page(
                        source_url=str(confirmed_source.get("url") or ""),
                        candidate_title=a.get("title", ""),
                        candidate_manufacturer=a.get("manufacturer", ""),
                        used_urls=used_grounding_urls,
                    )
                    if resolved_better_url:
                        normalized_url = resolved_better_url
                if not normalized_url and raw_url:
                    normalized_url = self._validate_external_product_url(
                        raw_url,
                        title=a.get("title", ""),
                        manufacturer=a.get("manufacturer", ""),
                    )
                if not normalized_url and confirmed_source.get("url"):
                    normalized_url = self._resolve_product_url_from_source_page(
                        source_url=str(confirmed_source.get("url") or ""),
                        candidate_title=a.get("title", ""),
                        candidate_manufacturer=a.get("manufacturer", ""),
                        used_urls=used_grounding_urls,
                    )
                if raw_url and not normalized_url:
                    logger.warning(
                        "[AnalogService] Discarded invalid AI product URL for '%s': %s",
                        a.get("title", ""),
                        str(raw_url)[:200],
                    )
                if normalized_url:
                    used_grounding_urls.add(normalized_url)
                url_note = ""
                url_status = "verified" if normalized_url else "not_found"
                if not normalized_url:
                    url_note = (
                        "Прямая ссылка на карточку товара не найдена или не подтверждена. "
                        "Кандидат подтвержден интернет-источником, но открыть конкретную страницу товара не удалось."
                    )
                source_url = str(confirmed_source.get("url") or "")
                source_title = str(confirmed_source.get("title") or preferred_source_title or "")
                source_url_status = "verified" if source_url else "not_found"
                source_url_note = ""
                if not source_url:
                    source_url_note = (
                        "Источник, по которому ИИ предложил этот товар, не удалось открыть повторно. "
                        "Карточка показана как неполностью подтвержденная."
                    )
                candidate = {
                    "id": None,
                    "title": a.get("title", ""),
                    "manufacturer": a.get("manufacturer", ""),
                    "category": a.get("material_type", ""),
                    "material_type": a.get("material_type", ""),
                    "price": a.get("price"),
                    "price_unit": a.get("price_unit", ""),
                    "specs": a.get("specs", {}),
                    "url": normalized_url,
                    "url_status": url_status,
                    "url_note": url_note,
                    "source_title": source_title,
                    "source_url": source_url,
                    "source_url_status": source_url_status,
                    "source_url_note": source_url_note,
                    "description": a.get("match_reason", ""),
                    "source": "ai_search",
                }

                passes_filters, filter_reasons = self._passes_hard_filters(
                    candidate,
                    clean_q,
                    requirements or "",
                    reference_profile=reference_profile,
                )
                if not passes_filters:
                    continue

                score, reasons = self._score_product(
                    candidate,
                    clean_q,
                    requirements or "",
                    reference_profile=reference_profile,
                )
                if score < min_score:
                    continue

                match_reasons = []
                for reason in filter_reasons + reasons:
                    if reason not in match_reasons:
                        match_reasons.append(reason)

                candidate["match_score"] = self._normalize_match_score(score)
                candidate["match_reason"] = "; ".join(match_reasons[:5])
                if candidate["match_reason"] and not candidate.get("description"):
                    candidate["description"] = candidate["match_reason"]
                result.append(candidate)

            logger.info(f"[AnalogService] AI found {len(result)} analogs for '{query}'")
            return result, ""

        except json.JSONDecodeError as e:
            logger.error(f"[AnalogService] JSON parse error: {e}", exc_info=True)
            return [], f"AI вернул невалидный JSON: {e}"
        except Exception as e:
            logger.error(f"[AnalogService] AI search error: {e}", exc_info=True)
            return [], str(e)

    # ─────────────────────────────────────────────────────────────────────────
    # КОМБИНИРОВАННЫЙ ПОИСК
    # ─────────────────────────────────────────────────────────────────────────

    async def search_analogs(
        self,
        query: str,
        requirements: str = None,
        use_ai: bool = True,
        limit: int = 10
    ) -> dict:
        """
        Главный метод: комбинированный поиск аналогов.

        Алгоритм:
          1. Очищаем запрос
          2. Ищем в локальной БД
          3. Если AI включён — передаём контекст из БД в AI промпт
          4. AI ищет дополнительные аналоги с учётом того что уже есть в БД
          5. Объединяем результаты, убираем дубли

        Возвращает:
          {
            "query": "очищенный запрос",
            "original_query": "исходный запрос",
            "local_results": [...],
            "ai_results": [...],
            "total": N,
            "ai_error": "..."
          }
        """
        # Очищаем запрос
        clean_query = self._clean_search_query(query)
        logger.info(
            f"[AnalogService] Combined search: "
            f"raw='{query[:60]}' → clean='{clean_query}' | "
            f"mode={'ai+db' if use_ai else 'db_only'} | limit={limit}"
        )

        reference_profile = await asyncio.to_thread(
            self._resolve_reference_profile,
            clean_query,
            requirements or "",
            allow_ai_lookup=False,
        )

        # Сначала ищем в локальной БД, не тратя AI-квоту на восстановление
        # референса, если база уже дает пригодный предварительный список.
        local_results = await asyncio.to_thread(
            self.search_local_db,
            clean_query,
            limit=limit,
            requirements=requirements,
            reference_profile=reference_profile,
        )

        query_requires_reference_lookup = self._query_requires_reference_lookup(
            clean_query,
            requirements or "",
        )
        if query_requires_reference_lookup and not reference_profile:
            logger.info(
                "[AnalogService] Reference profile lookup is DB-only. "
                "No local reference profile found for explicit brand/mark query."
            )
        elif use_ai and not reference_profile and not local_results:
            logger.info(
                "[AnalogService] AI reference profile lookup skipped: query has no explicit brand/article identity."
            )
        elif use_ai and not reference_profile and local_results:
            logger.info(
                "[AnalogService] AI reference profile lookup skipped: local DB already returned %s candidates.",
                len(local_results),
            )

        effective_requirements = self._augment_requirements_with_reference(
            requirements,
            reference_profile,
        )

        # AI поиск с передачей контекста из БД
        ai_results = []
        ai_error = ""
        if use_ai:
            # Передаём в AI что уже есть в БД — чтобы он не дублировал
            ai_results, ai_error = await self.search_ai(
                query=clean_query,
                requirements=effective_requirements,
                max_results=5,
                local_db_products=local_results,
                reference_profile=reference_profile,
            )

        # Убираем дубли между local и ai результатами
        seen_titles = {
            self._normalize_title_for_dedup(r.get("title", ""))
            for r in local_results
            if r.get("title")
        }
        ai_unique = []
        for r in ai_results:
            normalized_title = self._normalize_title_for_dedup(r.get("title", ""))
            if normalized_title and normalized_title not in seen_titles:
                ai_unique.append(r)
                seen_titles.add(normalized_title)

        validation_error = ""
        validation_summary = ""
        combined_candidates = [
            {**item, "source": item.get("source") or "local_db"}
            for item in (local_results + ai_unique)
        ]
        # Grounded internet search and non-grounded AI validation use different
        # model profiles. Even if grounded search is temporarily unavailable (503),
        # the lighter validation step can still succeed and should enrich local results.
        if combined_candidates and use_ai:
            validated_candidates, validation_summary, validation_state = await self.ai_validate_candidates(
                query=clean_query,
                requirements=effective_requirements,
                candidates=combined_candidates,
            )
            if validation_state == "applied":
                local_results = [item for item in validated_candidates if item.get("source") == "local_db"]
                ai_unique = [item for item in validated_candidates if item.get("source") == "ai_search"]
                local_results.sort(key=self._candidate_sort_key, reverse=True)
                ai_unique.sort(key=self._candidate_sort_key, reverse=True)
                if not validated_candidates:
                    validation_error = validation_summary or "AI validation rejected all candidates"
            elif validation_state == "fallback":
                validation_summary = validation_summary or "AI validation unavailable"
                if not combined_candidates:
                    validation_error = validation_summary

        return {
            "query": clean_query,
            "original_query": query,
            "local_results": local_results,
            "ai_results": ai_unique,
            "total": len(local_results) + len(ai_unique),
            "ai_error": ai_error,
            "validation_error": validation_error,
            "validation_summary": validation_summary,
            "reference_profile": self._public_reference_profile(reference_profile),
        }

    # ─────────────────────────────────────────────────────────────────────────
    # СОХРАНЕНИЕ AI-РЕЗУЛЬТАТОВ В БД
    # ─────────────────────────────────────────────────────────────────────────

    def save_ai_result_to_db(self, product: dict) -> Optional[int]:
        """
        Сохраняет найденный AI аналог в локальную БД для будущего использования.
        Возвращает ID созданной записи или None при ошибке.
        """
        from sqlalchemy import text
        import json as json_lib
        try:
            with self.db_session_factory() as session:
                specs_payload = product.get("specs", {}) or {}
                specs_json = json_lib.dumps(specs_payload, ensure_ascii=False)
                normalized_category = self._normalize_text(
                    product.get("category", product.get("material_type", "")) or ""
                )
                description = product.get("description", "") or ""
                specs_text = self._normalize_text(
                    " ".join(f"{key} {value}" for key, value in specs_payload.items())
                )
                quality_score = 20
                if description:
                    quality_score += 10
                if specs_payload:
                    quality_score += min(len(specs_payload) * 2, 20)
                result = session.execute(
                    text(
                        "INSERT INTO products ("
                        "vendor, source_url, title, category, normalized_category, searchable_for_analogs, "
                        "material_type, price, price_currency, specs, specs_text, url, description, "
                        "quality_score, parse_version, is_active"
                        ") VALUES ("
                        ":vendor, :source_url, :title, :cat, :normalized_category, :searchable, "
                        ":mat, :price, :price_currency, :specs, :specs_text, :url, :desc, "
                        ":quality_score, :parse_version, 1"
                        ")"
                    ),
                    {
                        "vendor": "ai",
                        "source_url": self._normalize_external_url(product.get("url", "")),
                        "title": product.get("title", ""),
                        "cat": product.get("category", product.get("material_type", "")),
                        "normalized_category": normalized_category,
                        "searchable": 1,
                        "mat": product.get("material_type", ""),
                        "price": product.get("price"),
                        "price_currency": "RUB",
                        "specs": specs_json,
                        "specs_text": specs_text,
                        "url": self._normalize_external_url(product.get("url", "")),
                        "desc": description,
                        "quality_score": quality_score,
                        "parse_version": "ai-manual-entry",
                    }
                )
                session.commit()
                logger.info(f"[AnalogService] Saved AI result to DB: {product.get('title')}")
                return result.lastrowid
        except Exception as e:
            logger.error(f"[AnalogService] Error saving to DB: {e}")
            return None
