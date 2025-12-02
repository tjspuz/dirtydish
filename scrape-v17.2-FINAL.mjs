#!/usr/bin/env node

/**
 * POLK COUNTY RESTAURANT INSPECTION SCRAPER V17.1
 * 
 * V17.1 New Features:
 * - Refined risk scoring based on violation severity (15/8/3/1 points)
 * - Automatic facility type categorization
 * - More accurate risk levels for fairer assessment
 * 
 * V17 Fixes (from V16):
 * - Duplicate detection system to catch pagination loops
 * - Stops immediately if page verification fails after "..." click
 * - Increased page limit from 100 to 1000 for statewide searches
 */

import { chromium } from 'playwright';
import { writeFileSync, existsSync, readFileSync } from 'fs';

// ============= IMPROVED FACILITY CATEGORIZATION V17.2 =============
const FACILITY_CATEGORIES = {
  'School': {
    keywords: ['school', 'elementary', 'middle school', 'high school', 'preschool', 'daycare', 'day care', 'childcare', 'child care', 'university', 'college', 'academy', 'kindergarten', 'head start'],
    priority: 10
  },
  'Senior/Community Center': {
    keywords: ['nutrition program', 'senior center', 'elderly', 'retirement', 'assisted living', 'nursing home', 'care center', 'community center', 'senior living', 'memory care', 'adult day'],
    priority: 9
  },
  'Fast Food': {
    keywords: ['mcdonalds', 'mcdonald\'s', 'burger king', 'subway', 'taco bell', 'wendy', 'wendys', 'kfc', 'arby', 'domino', 'pizza hut', 'papa john', 'sonic', 'dairy queen', 'dq ', 'popeyes', 'chick-fil-a', 'chipotle', 'panera', 'jimmy john', 'qdoba', 'taco john', 'culver', 'raising cane', 'five guys', 'shake shack', 'whataburger', 'drive thru', 'drive-thru'],
    priority: 8
  },
  'Gas Station/Convenience': {
    keywords: ['casey', 'kum & go', 'kum and go', 'kwik trip', 'kwik star', 'quiktrip', 'quick trip', 'bp ', 'shell', 'git n go', 'git-n-go', 'conoco', 'phillips 66', '7-eleven', 'speedway', 'pilot', 'flying j', 'circle k', 'marathon', 'loves', 'gas station', 'convenience store', 'c-store', 'truck stop'],
    priority: 7
  },
  'Grocery Store': {
    keywords: ['hy-vee', 'aldi', 'walmart', 'target', 'fareway', 'whole foods', 'trader joe', 'fresh thyme', 'natural grocers', 'kroger', 'costco', 'sam\'s club', 'sams club', 'grocery store', 'grocery', 'market', 'supermarket', 'food mart', 'food market', 'asian market', 'asian grocery', 'international market', 'ethnic market'],
    priority: 6
  },
  'Bar/Brewery/Distillery': {
    keywords: ['bar', 'pub', 'brewery', 'brewing', 'brewpub', 'taproom', 'tavern', 'lounge', 'saloon', 'sports bar', 'nightclub', 'night club', 'distillery', 'winery', 'wine bar', 'cocktail lounge', 'cocktail bar'],
    priority: 5
  },
  'Coffee Shop/Cafe': {
    keywords: ['starbucks', 'caribou', 'dunkin', 'peet\'s', 'dutch bros', 'tim horton', 'coffee', 'cafe', 'café', 'espresso', 'coffee shop', 'coffee house', 'coffeehouse'],
    priority: 4
  },
  'Bakery/Dessert': {
    keywords: ['bakery', 'cupcake', 'donut', 'doughnut', 'ice cream', 'gelato', 'frozen yogurt', 'candy', 'chocolate', 'sweet', 'pastry', 'cookies', 'dessert', 'confection', 'cake', 'fudge', 'yogurt shop'],
    priority: 3
  },
  'Hotel/Lodging': {
    keywords: ['hotel', 'motel', 'inn', 'resort', 'lodge', 'embassy suites', 'holiday inn', 'marriott', 'hilton', 'hampton', 'hyatt', 'sheraton', 'radisson', 'comfort inn', 'best western', 'la quinta', 'courtyard', 'residence inn'],
    priority: 2
  },
  'Recreation Facility': {
    keywords: ['bowling', 'golf', 'country club', 'athletic club', 'fitness', 'gym', 'pool hall', 'billiards', 'skating', 'rink', 'laser tag', 'arcade', 'theater', 'cinema', 'ymca', 'ywca', 'pickleball', 'tennis club', 'sports complex'],
    priority: 1
  },
  'Food Truck/Mobile': {
    keywords: ['food truck', 'mobile', ' cart', 'food cart', 'trailer', 'vendor', 'mobile kitchen', 'mobile food'],
    priority: 0
  },
  'Catering': {
    keywords: ['catering', 'caterer', 'banquet', 'event catering'],
    priority: -1
  },
  'Restaurant': {
    keywords: [],
    priority: -999
  }
};

function categorizeFacility(name, addr = '') {
  const text = `${name} ${addr}`.toLowerCase();
  let bestMatch = { category: 'Restaurant', priority: -999 };
  
  for (const [category, config] of Object.entries(FACILITY_CATEGORIES)) {
    for (const keyword of config.keywords) {
      if (text.includes(keyword.toLowerCase())) {
        if (config.priority > bestMatch.priority) {
          bestMatch = { category, priority: config.priority };
          break;
        }
      }
    }
  }
  
  return bestMatch.category;
}

const CONFIG = {
  BASE_URL: 'https://iowa.safefoodinspection.com/Inspection/PublicInspectionSearch.aspx',
  CITY_NAME: 'Des Moines',
  START_DATE: '11/26/2024',
  END_DATE: '11/26/2025',
  TIMEOUT: 90000,
  HEADLESS: false,
  DEBUG: true,
  OUTPUT_FILE: 'raw-data.json',
  COUNTY: 'Polk'
};

const Logger = {
  info: (msg) => console.log(`ℹ️  ${msg}`),
  success: (msg) => console.log(`✅ ${msg}`),
  error: (msg) => console.error(`❌ ${msg}`),
  debug: (msg) => CONFIG.DEBUG && console.log(`🔍 ${msg}`),
  warn: (msg) => console.warn(`⚠️  ${msg}`)
};

// ============= VIOLATION SEVERITY CLASSIFICATION =============
const VIOLATION_SEVERITY = {
  HIGH_PRIORITY: ['3-501.16(A)(2)', '3-501.16(A)(1)', '3-401.11', '3-501.14', '3-302.11(A)(1)', '3-302.11(A)(2)', '4-601.11(A)', '2-301.14', '3-201.11', '3-202.15'],
  MEDIUM_PRIORITY: ['3-501.17', '2-501.11', '2-401.11', '4-702.11', '4-501.114', '3-603.11', '3-305.11'],
  LOW_PRIORITY: ['2-102.12(A)', '2-103.11(O)', '5-205.11', '5-202.12(A)', '4-501.11', '4-903.11'],
  CORE: ['7-102.11', '6-301.14', '6-301.12', '7-204.11', '6-301.11', '7-207.11(B)', '7-202.11', '7-201.11']
};

function classifyViolation(code) {
  const normalized = code.trim();
  
  // Check each severity level
  for (const highCode of VIOLATION_SEVERITY.HIGH_PRIORITY) {
    if (normalized.startsWith(highCode) || normalized.includes(highCode)) {
      return 'critical';
    }
  }
  for (const medCode of VIOLATION_SEVERITY.MEDIUM_PRIORITY) {
    if (normalized.startsWith(medCode) || normalized.includes(medCode)) {
      return 'critical';
    }
  }
  for (const lowCode of VIOLATION_SEVERITY.LOW_PRIORITY) {
    if (normalized.startsWith(lowCode) || normalized.includes(lowCode)) {
      return 'critical';
    }
  }
  for (const coreCode of VIOLATION_SEVERITY.CORE) {
    if (normalized.startsWith(coreCode) || normalized.includes(coreCode)) {
      return 'noncritical';
    }
  }
  
  // Default: use code prefix
  const critical = ['2-', '3-', '4-', '5-'];
  return critical.some(c => normalized.startsWith(c)) ? 'critical' : 'noncritical';
}

function getViolationPoints(code) {
  const normalized = code.trim();
  
  for (const highCode of VIOLATION_SEVERITY.HIGH_PRIORITY) {
    if (normalized.startsWith(highCode) || normalized.includes(highCode)) return 15;
  }
  for (const medCode of VIOLATION_SEVERITY.MEDIUM_PRIORITY) {
    if (normalized.startsWith(medCode) || normalized.includes(medCode)) return 8;
  }
  for (const lowCode of VIOLATION_SEVERITY.LOW_PRIORITY) {
    if (normalized.startsWith(lowCode) || normalized.includes(lowCode)) return 3;
  }
  for (const coreCode of VIOLATION_SEVERITY.CORE) {
    if (normalized.startsWith(coreCode) || normalized.includes(coreCode)) return 1;
  }
  
  // Default scoring
  const critical = ['2-', '3-', '4-', '5-'];
  return critical.some(c => normalized.startsWith(c)) ? 8 : 1;
}

function calculateRiskScore(violations, closureFlag, reinspectionFlag) {
  let score = 0;
  
  // Score each violation by refined severity
  violations.forEach(v => {
    const code = v.violationCode || v.code || '';
    score += getViolationPoints(code);
  });
  
  // Penalties for serious situations
  if (closureFlag) score += 50;
  if (reinspectionFlag) score += 10;
  
  return Math.min(score, 100);
}

function getRiskLevel(score) {
  if (score === 0) return 'EXCELLENT';
  if (score <= 5) return 'LOW';       // 1-5 points (minor issues only)
  if (score <= 20) return 'MEDIUM';   // 6-20 points (some concerns)
  if (score <= 40) return 'HIGH';     // 21-40 points (significant issues)
  return 'CRITICAL';                  // 41+ points (serious problems)
}

function getColor(score) {
  if (score >= 50) return 'crimson';
  if (score >= 30) return 'red';
  if (score >= 15) return 'orange';
  if (score > 0) return 'yellow';
  return 'green';
}

function extractCity(address) {
  if (!address) return '';
  
  const cityMatch = address.match(/([A-Z][A-Za-z\s]+),\s*IA\s+\d{5}/);
  if (cityMatch) {
    let city = cityMatch[1].trim();
    city = city.replace(/\b(ST|AVE|AVENUE|RD|ROAD|BLVD|BOULEVARD|DR|DRIVE|LN|LANE|WAY|PKWY|PARKWAY)\s*/gi, '').trim();
    return city;
  }
  
  const cities = ['Des Moines', 'West Des Moines', 'Ankeny', 'Urbandale', 'Johnston', 'Clive', 'Altoona', 'Pleasant Hill', 'Polk City', 'Windsor Heights'];
  for (const city of cities) {
    if (address.toLowerCase().includes(city.toLowerCase())) {
      return city;
    }
  }
  
  return '';
}

async function closeModal(page) {
  try {
    await page.keyboard.press('Escape');
    await page.waitForTimeout(500);
    
    const closeButton = await page.locator('input[value="Close"]').first();
    const isVisible = await closeButton.isVisible().catch(() => false);
    if (isVisible) {
      await closeButton.click({ timeout: 2000 }).catch(() => {});
      await page.waitForTimeout(500);
    }
    
    const overlay = await page.locator('#cboxOverlay').first();
    const overlayVisible = await overlay.isVisible().catch(() => false);
    if (overlayVisible) {
      await overlay.click({ timeout: 2000 }).catch(() => {});
      await page.waitForTimeout(500);
    }
    
    Logger.debug('  Modal closed');
  } catch (err) {
    Logger.debug(`  Modal close attempt: ${err.message}`);
  }
}

async function scrapeViolationsModal(page, establishmentName) {
  try {
    const modalVisible = await page.waitForSelector('#tbPublicInspectionMain', { 
      timeout: 5000, state: 'visible'
    }).catch(() => null);
    
    if (!modalVisible) {
      Logger.debug('  Modal did not appear');
      return null;
    }
    
    await page.waitForTimeout(500);
    
    const inspectionDateText = await page.locator('#MainContent_wucPublicInspectionViolations_lblHeader')
      .innerText().catch(() => '');
    
    const dateMatch = inspectionDateText.match(/(\d{1,2}\/\d{1,2}\/\d{4})/);
    const inspectionDate = dateMatch ? dateMatch[1] : '';
    
    const violationRows = await page.locator('tbody > tr:has(td [id*="lblRegulatorCodeType"])').all();
    
    Logger.debug(`  Found ${violationRows.length} violation row(s)`);
    
    const violations = [];
    const seenViolations = new Set();
    
    for (const row of violationRows) {
      try {
        const violationCode = await row.locator('[id*="lblRegulatorCodeType"]').innerText().catch(() => '');
        
        const codeExplanationDiv = await row.locator('[id*="pnlCodeExplanation"]').first();
        let codeExplanation = await codeExplanationDiv.innerText().catch(() => '');
        codeExplanation = codeExplanation.replace(/Code Explanation\s*/i, '').trim();
        
        const commentsDiv = await row.locator('[id*="pnlComments"]').first();
        let inspectorComments = await commentsDiv.innerText().catch(() => '');
        inspectorComments = inspectorComments
          .replace(/Inspector Comments\s*/i, '')
          .replace(/--Observation:\s*/i, '')
          .trim();
        
        if (violationCode) {
          const uniqueKey = `${violationCode}|${codeExplanation}|${inspectorComments}`;
          
          if (seenViolations.has(uniqueKey)) {
            Logger.debug(`  Duplicate detected, skipping: ${violationCode}`);
            continue;
          }
          
          seenViolations.add(uniqueKey);
          
          const section = classifyViolation(violationCode);
          
          violations.push({
            violationCode,
            codeExplanation,
            inspectorComments,
            section
          });
          
          Logger.debug(`  ✓ ${section.toUpperCase()}: ${violationCode}`);
        }
      } catch (err) {
        Logger.debug(`  Error parsing violation row: ${err.message}`);
      }
    }
    
    await closeModal(page);
    
    return { violations, inspectionDate };
    
  } catch (err) {
    Logger.debug(`  Error in scrapeViolationsModal: ${err.message}`);
    await closeModal(page);
    return null;
  }
}

async function scrapePageResults(page) {
  Logger.info('\n📄 Scraping current page...');
  
  const results = [];
  
  const rows = await page.locator('#MainContent_gvInspections > tbody > tr.GridItem, #MainContent_gvInspections > tbody > tr.GridAltItem').all();
  
  Logger.info(`Found ${rows.length} establishment rows`);
  
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    
    try {
      const cells = await row.locator('td').all();
      
      if (cells.length < 6) {
        Logger.debug(`Row ${i}: Skipping (insufficient columns: ${cells.length})`);
        continue;
      }
      
      const nameAddrCell = cells[0];
      const nameAddrHtml = await nameAddrCell.innerHTML();
      
      const nameMatch = nameAddrHtml.match(/^\s*(.+?)<br/i);
      const establishmentName = nameMatch ? nameMatch[1].replace(/<[^>]*>/g, '').trim() : '';
      
      const addrMatch = nameAddrHtml.match(/font-style:italic">([^<]+)<\/div>/i);
      let address = addrMatch ? addrMatch[1].trim() : '';
      
      const phoneMatch = nameAddrHtml.match(/font-style:italic">[^<]+<\/div>\s*<div[^>]*font-style:italic">([^<]+)<\/div>/i);
      let phone = phoneMatch ? phoneMatch[1].trim() : '';
      
      address = address.replace(/(\d{5})\s+\d{4}\s*$/, '$1').trim();
      phone = phone.replace(/[^\d-]+$/, '').trim();
      
      const city = extractCity(address);
      
      const zipMatch = address.match(/\b(\d{5})\b/);
      const zipCode = zipMatch ? zipMatch[1] : '';
      
      const inspectionDate = await cells[1].innerText();
      const inspectionType = await cells[2].innerText();
      
      if (!inspectionType.toLowerCase().includes('routine')) {
        Logger.debug(`Row ${i}: Skipping ${establishmentName} (${inspectionType})`);
        continue;
      }
      
      const violationsLink = await cells[4].locator('a').first();
      const violationsText = await violationsLink.innerText().catch(() => '');
      
      const hasViolations = violationsText.trim().length > 0;
      
      Logger.info(`\n[${i + 1}/${rows.length}] ${establishmentName}`);
      Logger.debug(`  Address: ${address}`);
      Logger.debug(`  City: ${city || 'NOT EXTRACTED'}`);
      Logger.debug(`  Phone: ${phone}`);
      Logger.debug(`  Date: ${inspectionDate}`);
      Logger.debug(`  Type: ${inspectionType}`);
      Logger.debug(`  Has violations: ${hasViolations}`);
      
      let violations = [];
      let closureFlag = false;
      let reinspectionFlag = false;
      
      if (hasViolations) {
        try {
          await violationsLink.click({ timeout: 10000 });
          await page.waitForTimeout(1500);
          
          const modalData = await scrapeViolationsModal(page, establishmentName);
          
          if (modalData) {
            violations = modalData.violations;
          }
        } catch (err) {
          Logger.error(`  Error clicking violations: ${err.message}`);
          await closeModal(page);
        }
      }
      
      closureFlag = inspectionType.toLowerCase().includes('closure');
      reinspectionFlag = inspectionType.toLowerCase().includes('follow') || 
                         inspectionType.toLowerCase().includes('reinspection');
      
      const criticalCount = violations.filter(v => v.section === 'critical').length;
      const noncriticalCount = violations.filter(v => v.section === 'noncritical').length;
      const totalViolations = violations.length;
      
      const riskScore = calculateRiskScore(violations, closureFlag, reinspectionFlag);
      const riskLevel = getRiskLevel(riskScore);
      const color = getColor(riskScore);
      const facilityType = categorizeFacility(establishmentName, address);
      
      results.push({
        name: establishmentName,
        addr: address,
        phone: phone,
        city: city,
        facilityType: facilityType,
        inspectionDate: inspectionDate,
        inspectionType: inspectionType,
        violations: violations,
        closureFlag: closureFlag,
        reinspectionFlag: reinspectionFlag,
        zipCode: zipCode,
        county: CONFIG.COUNTY,
        criticalCount: criticalCount,
        noncriticalCount: noncriticalCount,
        totalViolations: totalViolations,
        riskScore: riskScore,
        riskLevel: riskLevel,
        color: color,
        scrapedAt: new Date().toISOString()
      });
      
      Logger.success(`  Scraped: ${totalViolations} violations, Risk: ${riskLevel} (${riskScore}), Type: ${facilityType}`);
      
    } catch (err) {
      Logger.error(`Row ${i}: Error - ${err.message}`);
      await closeModal(page);
    }
  }
  
  return results;
}

async function scrapeCityWithDateRange(page, cityName, startDate, endDate) {
  Logger.info(`\n🔍 Searching: ${cityName} (${startDate} to ${endDate})`);
  
  await page.goto(CONFIG.BASE_URL, { waitUntil: 'networkidle' });
  await page.waitForTimeout(2000);
  
  Logger.info('Setting state to Iowa...');
  await page.selectOption('#MainContent_wucStateCountiesFS_ddlState', { value: '82' });
  await page.waitForTimeout(1500);
  
  Logger.info('Setting region to IOWA DIA...');
  await page.selectOption('#MainContent_wucStateCountiesFS_ddlCountyGroup', { value: '1' });
  await page.waitForTimeout(1500);
  
  Logger.info(`Entering city: ${cityName}`);
  await page.fill('#MainContent_txtCity', cityName);
  await page.waitForTimeout(500);
  
  Logger.info(`Setting date range: ${startDate} to ${endDate}`);
  await page.fill('#MainContent_dteInspectionBeginDate_txtDate', startDate);
  await page.waitForTimeout(300);
  await page.fill('#MainContent_dteInspectionEndDate_txtDate', endDate);
  await page.waitForTimeout(300);
  
  Logger.info('Clicking Search button...');
  await page.click('#MainContent_btnSearch');
  await page.waitForTimeout(3000);
  
  const noResults = await page.locator('text=No records found').first().isVisible().catch(() => false);
  
  if (noResults) {
    Logger.warn('No results found for this search');
    return [];
  }
  
  Logger.success('Results loaded!');
  
  let allResults = [];
  let pageNum = 1;
  
  // V17: Duplicate detection system
  const seenEstablishments = new Set();
  let consecutiveDuplicatePages = 0;
  const MAX_DUPLICATE_PAGES = 3;
  
  while (true) {
    Logger.info(`\n📄 ========== PAGE ${pageNum} ==========`);
    
    const pageResults = await scrapePageResults(page);
    
    // V17: Track duplicates on this page
    let duplicatesOnPage = 0;
    for (const result of pageResults) {
      const key = `${result.name}|${result.addr}|${result.inspectionDate}`;
      if (seenEstablishments.has(key)) {
        duplicatesOnPage++;
      } else {
        seenEstablishments.add(key);
      }
    }
    
    allResults = allResults.concat(pageResults);
    
    Logger.success(`Page ${pageNum}: Scraped ${pageResults.length} establishments (${duplicatesOnPage} duplicates, Total: ${allResults.length}, Unique: ${seenEstablishments.size})`);
    
    // V17: Check if entire page is duplicates (infinite loop detection)
    if (pageResults.length > 0 && duplicatesOnPage === pageResults.length) {
      consecutiveDuplicatePages++;
      Logger.warn(`⚠️  Page ${pageNum} is 100% duplicates (${consecutiveDuplicatePages}/${MAX_DUPLICATE_PAGES})`);
      
      if (consecutiveDuplicatePages >= MAX_DUPLICATE_PAGES) {
        Logger.error(`\n❌ PAGINATION LOOP DETECTED: ${MAX_DUPLICATE_PAGES} consecutive duplicate pages`);
        Logger.error(`   Stopping scrape to prevent infinite loop`);
        break;
      }
    } else {
      consecutiveDuplicatePages = 0; // Reset if we found new data
    }
    
    // HYBRID PAGINATION APPROACH
    // Get all GridPager elements and find the one that's part of main grid (not nested)
    const allPagers = await page.locator('tr.GridPager').all();
    
    let mainPager = null;
    for (const pager of allPagers) {
      // Check if this pager is inside a nested table
      const parentTable = await pager.evaluate(el => {
        let parent = el.parentElement;
        while (parent) {
          if (parent.tagName === 'TABLE') {
            return parent.id;
          }
          parent = parent.parentElement;
        }
        return null;
      });
      
      // The main pager's parent table should be MainContent_gvInspections
      if (parentTable === 'MainContent_gvInspections') {
        mainPager = pager;
        break;
      }
    }
    
    if (!mainPager) {
      Logger.warn('Could not find main pager');
      break;
    }
    
    // Get current page from main pager
    const currentPageSpan = await mainPager.locator('span').first();
    const currentPageText = await currentPageSpan.innerText().catch(() => '');
    const currentPage = parseInt(currentPageText) || 1;
    const nextPage = currentPage + 1;
    
    // V17: Verify we're on the expected page (catches pagination mismatches)
    if (currentPage !== pageNum) {
      Logger.error(`\n❌ PAGE MISMATCH: Expected page ${pageNum}, but found page ${currentPage}`);
      Logger.error(`   This indicates pagination loop. Stopping.`);
      break;
    }
    
    Logger.debug(`Current page: ${currentPage}, looking for page: ${nextPage}`);
    
    // Try to find next page link in main pager
    let nextPageLink = await mainPager.locator(`a:has-text("${nextPage}")`).first();
    let nextPageExists = await nextPageLink.isVisible().catch(() => false);
    
    // If next page link doesn't exist, check for "..." link
    if (!nextPageExists) {
      Logger.debug('Next page link not found, checking for "..." link');
      
      const ellipsisLink = await mainPager.locator('a:has-text("...")').first();
      const ellipsisExists = await ellipsisLink.isVisible().catch(() => false);
      
      if (ellipsisExists) {
        Logger.info('\n➡️  Clicking "..." to load more pages...');
        await ellipsisLink.click();
        
        // IMPORTANT: Clicking "..." automatically navigates to the next page
        // So we don't need to click again - just wait for page to load
        await page.waitForTimeout(3000);
        
        // V17: Wait for page number to update - MUST succeed or stop
        try {
          await page.waitForFunction(
            (expectedPage) => {
              const mainGrid = document.querySelector('#MainContent_gvInspections');
              if (!mainGrid) return false;
              
              const pagers = document.querySelectorAll('tr.GridPager');
              for (const pager of pagers) {
                let parent = pager.parentElement;
                while (parent) {
                  if (parent === mainGrid) {
                    const span = pager.querySelector('span');
                    return span && parseInt(span.innerText) === expectedPage;
                  }
                  parent = parent.parentElement;
                }
              }
              return false;
            },
            nextPage,
            { timeout: 10000 }
          );
          Logger.debug(`✓ Automatically navigated to page ${nextPage}`);
        } catch (err) {
          Logger.error(`\n❌ VERIFICATION FAILED after clicking "..."`);
          Logger.error(`   Expected page ${nextPage}, but validation timed out`);
          Logger.error(`   This indicates navigation failure. Stopping to prevent loop.`);
          break; // V17: STOP instead of continuing
        }
        
        // Continue to next iteration - page has already changed
        await page.waitForTimeout(2000);
        pageNum++;
        continue;
      } else {
        Logger.info('\n✓ No more pages (no next page or "..." link)');
        break;
      }
    }
    
    // Click next page (normal pagination)
    Logger.info(`\n➡️  Going to page ${nextPage}...`);
    await nextPageLink.click();
    
    // Wait for page number to update in main pager
    try {
      await page.waitForFunction(
        () => {
          // Find the main grid's pager
          const mainGrid = document.querySelector('#MainContent_gvInspections');
          if (!mainGrid) return false;
          
          // Find all GridPager rows
          const pagers = document.querySelectorAll('tr.GridPager');
          for (const pager of pagers) {
            // Check if this pager belongs to main grid
            let parent = pager.parentElement;
            while (parent) {
              if (parent === mainGrid) {
                // This is the main pager
                const span = pager.querySelector('span');
                return span && parseInt(span.innerText) > 1; // Wait for it to change from 1
              }
              parent = parent.parentElement;
            }
          }
          return false;
        },
        { timeout: 10000 }
      );
      Logger.debug(`✓ Page changed to ${nextPage}`);
    } catch (err) {
      Logger.warn(`Page number validation timed out, continuing...`);
    }
    
    // Wait for grid to reload
    await page.waitForTimeout(2000);
    
    pageNum++;
    
    if (pageNum > 1000) {
      Logger.warn('⚠️  Reached page limit (1000), stopping');
      break;
    }
  }
  
  return allResults;
}

function keepMostRecentInspections(data) {
  Logger.info('\n🔄 Filtering to most recent inspections...');
  
  const byEstablishment = new Map();
  
  for (const record of data) {
    const key = `${record.name.toLowerCase().trim()}|${record.addr.toLowerCase().trim()}`;
    
    if (!byEstablishment.has(key)) {
      byEstablishment.set(key, record);
    } else {
      const existing = byEstablishment.get(key);
      const existingDate = new Date(existing.inspectionDate);
      const currentDate = new Date(record.inspectionDate);
      
      // If current record has newer fields (like facilityType), update them
      if (record.facilityType && !existing.facilityType) {
        existing.facilityType = record.facilityType;
        Logger.debug(`Updated facilityType for ${record.name}: ${record.facilityType}`);
      }
      
      // If inspection date is newer, replace entirely
      if (currentDate > existingDate) {
        Logger.debug(`Replacing ${record.name}: ${existing.inspectionDate} -> ${record.inspectionDate}`);
        byEstablishment.set(key, record);
      }
    }
  }
  
  const filtered = Array.from(byEstablishment.values());
  Logger.success(`Kept ${filtered.length} unique (from ${data.length} total)`);
  
  return filtered;
}

function mergeWithExistingData(newData, existingFilePath) {
  if (!existsSync(existingFilePath)) {
    Logger.info('No existing data file found, using new data only');
    return newData;
  }
  
  Logger.info(`\n🔄 Merging with existing data from ${existingFilePath}...`);
  
  const existingData = JSON.parse(readFileSync(existingFilePath, 'utf-8'));
  Logger.info(`Existing records: ${existingData.length}`);
  Logger.info(`New records: ${newData.length}`);
  
  const combined = [...existingData, ...newData];
  const merged = keepMostRecentInspections(combined);
  
  Logger.success(`Merged result: ${merged.length} unique establishments`);
  
  return merged;
}

async function main() {
  let browser;
  
  try {
    Logger.info('🚀 Polk County Restaurant Scraper V17.2 - IMPROVED CATEGORIZATION\n');
    Logger.info(`📅 Date Range: ${CONFIG.START_DATE} - ${CONFIG.END_DATE}`);
    Logger.info(`📍 City: ${CONFIG.CITY_NAME}\n`);
    
    browser = await chromium.launch({ 
      headless: CONFIG.HEADLESS,
      timeout: 60000,
      args: ['--disable-blink-features=AutomationControlled']
    });
    
    const context = await browser.newContext({ 
      viewport: { width: 1920, height: 1080 },
      userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    });
    
    const page = await context.newPage();
    page.setDefaultTimeout(60000);
    
    const allData = await scrapeCityWithDateRange(
      page, 
      CONFIG.CITY_NAME, 
      CONFIG.START_DATE, 
      CONFIG.END_DATE
    );
    
    if (allData.length === 0) {
      Logger.error('\n⚠️  No data scraped!');
      return;
    }
    
    const finalData = mergeWithExistingData(allData, CONFIG.OUTPUT_FILE);
    
    // Get absolute path for clarity
    const path = await import('path');
    const { statSync } = await import('fs');
    const absolutePath = path.resolve(CONFIG.OUTPUT_FILE);
    
    writeFileSync(CONFIG.OUTPUT_FILE, JSON.stringify(finalData, null, 2));
    
    // Verify file was written
    try {
      const stats = statSync(CONFIG.OUTPUT_FILE);
      const fileSizeKB = (stats.size / 1024).toFixed(2);
      Logger.success(`\n✨ Complete! ${finalData.length} facilities saved`);
      Logger.success(`📁 File: ${absolutePath}`);
      Logger.success(`📏 Size: ${fileSizeKB} KB`);
    } catch (err) {
      Logger.error(`⚠️  File written but couldn't verify: ${err.message}`);
    }
    
    const excellent = finalData.filter(r => r.riskLevel === 'EXCELLENT').length;
    const low = finalData.filter(r => r.riskLevel === 'LOW').length;
    const medium = finalData.filter(r => r.riskLevel === 'MEDIUM').length;
    const high = finalData.filter(r => r.riskLevel === 'HIGH').length;
    const critical = finalData.filter(r => r.riskLevel === 'CRITICAL').length;
    
    Logger.info(`\n📊 Risk Distribution:`);
    Logger.info(`   🟢 Excellent: ${excellent}`);
    Logger.info(`   🟡 Low: ${low}`);
    Logger.info(`   🟠 Medium: ${medium}`);
    Logger.info(`   🔴 High: ${high}`);
    Logger.info(`   🔴 Critical: ${critical}`);
    
    const cities = new Map();
    finalData.forEach(r => {
      const city = r.city || 'Unknown';
      cities.set(city, (cities.get(city) || 0) + 1);
    });
    
    Logger.info(`\n📍 City Breakdown:`);
    for (const [city, count] of Array.from(cities.entries()).sort((a, b) => b[1] - a[1])) {
      Logger.info(`   ${city}: ${count}`);
    }
    
  } catch (err) {
    Logger.error(`\n💥 Fatal error: ${err.message}`);
    Logger.error(err.stack);
  } finally {
    if (browser) await browser.close();
  }
}

main().catch(console.error);
