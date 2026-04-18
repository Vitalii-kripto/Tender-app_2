const { chromium } = require('playwright');

(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  
  try {
    // Load google translate or something maybe? No, let's just go directly and see if we get blocked. The playwright browser might slip through.
    const res = await page.goto('https://zakupki.gov.ru/api/control/v1/nsi/okpd2?searchString=23&pageNumber=1&recordsPerPage=50', { waitUntil: 'load', timeout: 15000 });
    const content = await page.content();
    console.log(content.substring(0, 5000));
  } catch (e) {
    console.log("Error:", e.message);
  }
  await browser.close();
})();
