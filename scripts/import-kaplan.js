#!/usr/bin/env node
/**
 * Import 1976-1985 SHR data from Jacob Kaplan's CSV into the existing SQLite DB.
 * Maps human-readable values to match the existing schema.
 */

const Database = require('better-sqlite3');
const fs = require('fs');
const path = require('path');
const readline = require('readline');

const dbPath = path.join(__dirname, '..', 'data', 'shr.db');
const csvPath = path.join(__dirname, '..', 'data', 'kaplan', 'SHR_1976_2015.csv');
const db = new Database(dbPath);

// ── CSV parser (handles quoted fields) ──
function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') { inQuotes = !inQuotes; }
    else if (ch === ',' && !inQuotes) { result.push(current); current = ''; }
    else { current += ch; }
  }
  result.push(current);
  return result;
}

// ── State name → abbreviation ──
const STATE_ABBREV = {
  'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
  'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
  'district of columbia': 'DC', 'florida': 'FL', 'georgia': 'GA', 'guam': 'GU',
  'hawaii': 'HI', 'idaho': 'ID', 'illinois': 'IL', 'indiana': 'IN',
  'iowa': 'IA', 'kansas': 'KS', 'kentucky': 'KY', 'louisiana': 'LA',
  'maine': 'ME', 'maryland': 'MD', 'massachusetts': 'MA', 'michigan': 'MI',
  'minnesota': 'MN', 'mississippi': 'MS', 'missouri': 'MO', 'montana': 'MT',
  'nebraska': 'NE', 'nevada': 'NV', 'new hampshire': 'NH', 'new jersey': 'NJ',
  'new mexico': 'NM', 'new york': 'NY', 'north carolina': 'NC', 'north dakota': 'ND',
  'ohio': 'OH', 'oklahoma': 'OK', 'oregon': 'OR', 'pennsylvania': 'PA',
  'rhode island': 'RI', 'south carolina': 'SC', 'south dakota': 'SD', 'tennessee': 'TN',
  'texas': 'TX', 'utah': 'UT', 'vermont': 'VT', 'virginia': 'VA',
  'washington': 'WA', 'west virginia': 'WV', 'wisconsin': 'WI', 'wyoming': 'WY',
  'puerto rico': 'PR', 'virgin islands': 'VI', 'canal zone': 'CZ',
};

// ── Month name → number ──
const MONTH_NUM = {
  'january': 1, 'february': 2, 'march': 3, 'april': 4,
  'may': 5, 'june': 6, 'july': 7, 'august': 8,
  'september': 9, 'october': 10, 'november': 11, 'december': 12,
};

// ── Weapon mapping ──
const WEAPON_MAP = {
  'handgun': ['Handgun', 'Firearm'],
  'rifle': ['Rifle', 'Firearm'],
  'shotgun': ['Shotgun', 'Firearm'],
  'other or unknown firearm': ['Firearm (type not stated)', 'Firearm'],
  'knife or cutting instrument': ['Knife/Cutting Instrument', 'Knife/Cutting'],
  'blunt object - hammer, club, etc': ['Blunt Object', 'Blunt Object'],
  'personal weapons, includes beating': ['Personal Weapons', 'Personal Weapons'],
  'strangulation, includes hanging': ['Strangulation/Hanging', 'Strangulation'],
  'asphyxiation, includes gas': ['Asphyxiation', 'Asphyxiation'],
  'fire': ['Fire', 'Fire'],
  'poison - does not include gas': ['Poison', 'Poison'],
  'narcotics or drugs': ['Narcotics/Drugs', 'Narcotics/Drugs'],
  'drowning': ['Drowning', 'Other'],
  'explosives': ['Explosives', 'Other'],
  'pushed or thrown out of window': ['Pushed/Thrown Out Window', 'Other'],
  'other or unknown': ['Other/Unknown', 'Other'],
};

// ── Circumstance mapping ──
const CIRC_MAP = {
  'rape': ['Rape', 'Felony Type'],
  'robbery': ['Robbery', 'Felony Type'],
  'burglary': ['Burglary', 'Felony Type'],
  'larceny': ['Larceny', 'Felony Type'],
  'motor vehicle theft': ['Motor Vehicle Theft', 'Felony Type'],
  'arson': ['Arson', 'Felony Type'],
  'prostitution and commercialized vice': ['Prostitution', 'Felony Type'],
  'other sex offense': ['Other Sex Offenses', 'Felony Type'],
  'narcotic drug laws': ['Narcotic Drug Laws', 'Felony Type'],
  'gambling': ['Gambling', 'Felony Type'],
  'all suspected felony type': ['All Suspected Felony', 'Felony Type'],
  'other arguments': ['Other Arguments', 'Other Than Felony'],
  'argument over money or property': ['Argument Over Money/Property', 'Other Than Felony'],
  'lovers triangle': ["Lover's Triangle", 'Other Than Felony'],
  'brawl due to influence of alcohol': ['Brawl (Alcohol)', 'Other Than Felony'],
  'brawl due to influence of narcotics': ['Brawl (Narcotics)', 'Other Than Felony'],
  'gangland killing': ['Gangland Killings', 'Other Than Felony'],
  'juvenile gang killing': ['Juvenile Gang Killings', 'Other Than Felony'],
  'institution killing': ['Institutional Killings', 'Other Than Felony'],
  'sniper attack': ['Sniper Attack', 'Other Than Felony'],
  'other - not specified': ['Other', 'Other Than Felony'],
  'all other manslaughter by negligence': ['Other Manslaughter by Negligence', 'Manslaughter by Negligence'],
  'children playing with gun': ['Children Playing With Gun', 'Manslaughter by Negligence'],
  'gun-cleaning death - other than self-inflicted': ['Gun-Cleaning Death', 'Manslaughter by Negligence'],
  'child killed by babysitter': ['Child Killed by Babysitter', 'Other Than Felony'],
  'victim shot in hunting accident': ['Hunting Accident', 'Manslaughter by Negligence'],
  'felon killed by police': ['Felon Killed by Police', 'Justifiable Homicide'],
  'felon killed by private citizen': ['Felon Killed by Private Citizen', 'Justifiable Homicide'],
  'all instances': ['Unknown', 'Unknown'],
};

// ── Relationship mapping ──
const REL_MAP = {
  'husband': ['Husband', 'Family'], 'wife': ['Wife', 'Family'],
  'common-law husband': ['Common-Law Husband', 'Family'], 'common-law wife': ['Common-Law Wife', 'Family'],
  'mother': ['Mother', 'Family'], 'father': ['Father', 'Family'],
  'son': ['Son', 'Family'], 'daughter': ['Daughter', 'Family'],
  'brother': ['Brother', 'Family'], 'sister': ['Sister', 'Family'],
  'in-law': ['In-Law', 'Family'], 'stepfather': ['Stepfather', 'Family'],
  'stepmother': ['Stepmother', 'Family'], 'stepson': ['Stepson', 'Family'],
  'stepdaughter': ['Stepdaughter', 'Family'], 'other family': ['Other Family', 'Family'],
  'ex-husband': ['Ex-Husband', 'Family'], 'ex-wife': ['Ex-Wife', 'Family'],
  'acquaintance': ['Acquaintance', 'Known'], 'friend': ['Friend', 'Known'],
  'boyfriend': ['Boyfriend', 'Known'], 'girlfriend': ['Girlfriend', 'Known'],
  'neighbor': ['Neighbor', 'Known'], 'employee': ['Employee', 'Known'],
  'employer': ['Employer', 'Known'], 'homosexual relationship': ['Homosexual Relationship', 'Known'],
  'other - known to victim': ['Other Known', 'Known'],
  'stranger': ['Stranger', 'Stranger'],
  'relationship not determined': ['Unknown', 'Unknown'],
};

// ── Homicide type mapping ──
const HTYPE_MAP = {
  'murder and non-negligent manslaughter': 'Murder',
  'manslaughter by negligence': 'Manslaughter',
};

// ── Sex mapping ──
function mapSex(s) {
  s = (s || '').toLowerCase().trim();
  if (s === 'male') return 'Male';
  if (s === 'female') return 'Female';
  return 'Unknown';
}

// ── Race mapping ──
function mapRace(r) {
  r = (r || '').toLowerCase().trim();
  if (r === 'white') return 'White';
  if (r === 'black') return 'Black';
  if (r === 'american indian or alaskan native') return 'American Indian/Alaska Native';
  if (r === 'asian or pacific islander') return 'Asian/Pacific Islander';
  return 'Unknown';
}

// ── Ethnicity mapping ──
function mapEthnicity(e) {
  e = (e || '').toLowerCase().trim();
  if (e === 'hispanic') return 'Hispanic';
  if (e === 'not hispanic') return 'Not Hispanic';
  return 'Unknown';
}

// ── Parse age ──
function parseAge(a) {
  if (!a || a === 'NA' || a === 'unknown') return null;
  // Handle "birth to 1 year old", "99 years old or older"
  if (a.includes('birth')) return 0;
  const num = parseInt(a);
  return isNaN(num) ? null : num;
}

async function main() {
  // Delete existing 1976-1985 data if any
  const existing = db.prepare('SELECT COUNT(*) as n FROM incidents WHERE year BETWEEN 1976 AND 1985').get();
  if (existing.n > 0) {
    console.log(`Deleting ${existing.n} existing incidents for 1976-1985...`);
    db.prepare('DELETE FROM victims WHERE incident_id IN (SELECT id FROM incidents WHERE year BETWEEN 1976 AND 1985)').run();
    db.prepare('DELETE FROM incidents WHERE year BETWEEN 1976 AND 1985').run();
  }

  const insertIncident = db.prepare(`
    INSERT INTO incidents (year, month, state, ori, agency, population, homicide_type, situation,
      weapon, weapon_group, circumstance, circumstance_group,
      relationship, relationship_group)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const insertVictim = db.prepare(`
    INSERT INTO victims (incident_id, victim_num, age, age_num, sex, race, ethnicity)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `);

  const rl = readline.createInterface({ input: fs.createReadStream(csvPath) });
  let header = null;
  let incidents = 0, victims = 0, skipped = 0;
  const yearCounts = {};

  const insertMany = db.transaction((rows) => {
    for (const row of rows) {
      const { inc, vics } = row;
      const result = insertIncident.run(...inc);
      const incId = result.lastInsertRowid;
      for (const v of vics) {
        insertVictim.run(incId, ...v);
      }
      incidents++;
      victims += vics.length;
    }
  });

  let batch = [];

  for await (const line of rl) {
    const cols = parseCSVLine(line);
    if (!header) { header = cols; continue; }

    const get = (name) => {
      const idx = header.indexOf(name);
      return idx >= 0 ? (cols[idx] || '').trim() : '';
    };

    const year = parseInt(get('year'));
    if (isNaN(year) || year < 1976 || year > 1985) continue;

    const stateRaw = get('state').toLowerCase();
    const state = STATE_ABBREV[stateRaw];
    if (!state) { skipped++; continue; }

    const month = MONTH_NUM[get('month_of_offense').toLowerCase()] || null;
    const ori = get('ori_code').toUpperCase();
    const agency = get('agency_name').toUpperCase();

    const htypeRaw = get('homicide_type').toLowerCase();
    const homicideType = HTYPE_MAP[htypeRaw];
    if (!homicideType) { skipped++; continue; }

    const situation = get('situation');
    const population = parseInt(get('population')) || 0;

    // Weapon (from offender_1)
    const weaponRaw = get('offender_1_weapon').toLowerCase();
    const [weapon, weaponGroup] = WEAPON_MAP[weaponRaw] || ['Unknown', 'Other'];

    // Circumstance (from offender_1)
    const circRaw = get('offender_1_circumstance').toLowerCase();
    const [circumstance, circGroup] = CIRC_MAP[circRaw] || ['Unknown', 'Unknown'];

    // Relationship (from offender_1)
    const relRaw = get('offender_1_relationship').toLowerCase();
    const [relationship, relGroup] = REL_MAP[relRaw] || ['Unknown', 'Unknown'];

    // Build victim list
    const vics = [];
    for (let vi = 1; vi <= 11; vi++) {
      const ageRaw = get(`victim_${vi}_age`);
      const sexRaw = get(`victim_${vi}_sex`);
      if (!ageRaw && !sexRaw) break;  // No more victims
      if (sexRaw.toLowerCase() === 'na' && ageRaw.toLowerCase() === 'na') break;

      const ageNum = parseAge(ageRaw);
      const ageStr = ageNum !== null ? String(ageNum) : 'Unknown';
      vics.push([vi, ageStr, ageNum, mapSex(sexRaw), mapRace(get(`victim_${vi}_race`)), mapEthnicity(get(`victim_${vi}_ethnicity`))]);
    }

    if (vics.length === 0) {
      // At minimum, create one unknown victim
      vics.push([1, 'Unknown', null, 'Unknown', 'Unknown', 'Unknown']);
    }

    yearCounts[year] = (yearCounts[year] || 0) + 1;

    batch.push({
      inc: [year, month, state, ori, agency, population, homicideType, situation,
            weapon, weaponGroup, circumstance, circGroup, relationship, relGroup],
      vics,
    });

    if (batch.length >= 5000) {
      insertMany(batch);
      batch = [];
    }
  }

  if (batch.length > 0) insertMany(batch);

  for (const y of Object.keys(yearCounts).sort()) {
    const vc = db.prepare('SELECT COUNT(*) as n FROM victims v JOIN incidents i ON v.incident_id = i.id WHERE i.year = ?').get(parseInt(y));
    console.log(`${y}: ${yearCounts[y]} incidents, ${vc.n} victims`);
  }

  const total = db.prepare('SELECT COUNT(*) as n FROM incidents').get();
  const totalV = db.prepare('SELECT COUNT(*) as n FROM victims').get();
  console.log(`\nDone! Total DB: ${total.n} incidents, ${totalV.n} victims, ${skipped} skipped`);
  db.close();
}

main().catch(e => { console.error(e); process.exit(1); });
