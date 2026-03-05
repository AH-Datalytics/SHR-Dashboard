#!/usr/bin/env node
/**
 * Check for SHR data updates from the FBI Crime Data Explorer.
 * Downloads the current year and previous year's zip files,
 * compares to existing data, and signals if an update is needed.
 *
 * Usage: node scripts/check-update.js
 * Exit code 0 = updated, 1 = error, 2 = no change
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const crypto = require('crypto');

const dataDir = path.join(__dirname, '..', 'data');

// Get a signed S3 URL from the CDE for a given year's SHR zip
function getSignedUrl(year) {
  return new Promise((resolve, reject) => {
    const url = `https://cde.ucr.cjis.gov/LATEST/s3/signedurl?key=nibrs/master/shr/shr-${year}.zip`;
    https.get(url, { headers: { 'Accept': 'application/json' } }, (res) => {
      let body = '';
      res.on('data', d => body += d);
      res.on('end', () => {
        if (res.statusCode !== 200) {
          return reject(new Error(`CDE returned ${res.statusCode} for year ${year}`));
        }
        try {
          // Response might be JSON with a url field, or just a redirect URL
          const parsed = JSON.parse(body);
          resolve(parsed.url || parsed);
        } catch {
          // Might be a plain URL string
          resolve(body.trim().replace(/^"|"$/g, ''));
        }
      });
    }).on('error', reject);
  });
}

// Download a URL to a buffer, following redirects
function download(url) {
  return new Promise((resolve, reject) => {
    const client = url.startsWith('https') ? https : http;
    client.get(url, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return download(res.headers.location).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) {
        return reject(new Error(`HTTP ${res.statusCode} for ${url.substring(0, 100)}...`));
      }
      const chunks = [];
      res.on('data', d => chunks.push(d));
      res.on('end', () => resolve(Buffer.concat(chunks)));
    }).on('error', reject);
  });
}

function md5(buf) {
  return crypto.createHash('md5').update(buf).digest('hex');
}

async function main() {
  const now = new Date();
  const currentYear = now.getFullYear();
  const prevYear = currentYear - 1;
  const yearsToCheck = [prevYear, currentYear];

  let anyChanged = false;

  for (const year of yearsToCheck) {
    const zipPath = path.join(dataDir, `shr-${year}.zip`);
    const existingHash = fs.existsSync(zipPath) ? md5(fs.readFileSync(zipPath)) : null;

    console.log(`Checking year ${year}...`);
    try {
      const signedUrl = await getSignedUrl(year);
      const buf = await download(signedUrl);

      if (buf.length < 100) {
        console.log(`  Year ${year}: response too small (${buf.length} bytes), skipping`);
        continue;
      }

      const newHash = md5(buf);
      if (newHash === existingHash) {
        console.log(`  Year ${year}: no change (${buf.length} bytes, md5=${newHash.substring(0, 8)})`);
      } else {
        console.log(`  Year ${year}: UPDATED (${buf.length} bytes, md5=${newHash.substring(0, 8)}, was=${existingHash ? existingHash.substring(0, 8) : 'new'})`);
        fs.writeFileSync(zipPath, buf);
        anyChanged = true;
      }
    } catch (e) {
      console.error(`  Year ${year}: ${e.message}`);
    }
  }

  if (anyChanged) {
    console.log('\nData changed! Re-parse and re-aggregate needed.');
    process.exit(0);
  } else {
    console.log('\nNo changes detected.');
    process.exit(2);
  }
}

main().catch(e => { console.error(e); process.exit(1); });
