#!/usr/bin/env node
/**
 * Parse FBI SHR fixed-length master files into SQLite.
 *
 * Record layout: 268 chars per record (1-indexed positions from PDF)
 *   1       : Identifier ("6")
 *   2-3     : State code (01-62)
 *   4-10    : ORI code
 *   11-12   : Group
 *   13      : Division (0-9)
 *   14-15   : Year (2-digit)
 *   16-24   : Population (N9)
 *   25-27   : County code
 *   28-30   : MSA code
 *   31      : MSA Indication
 *   32-55   : Agency Name (A24)
 *   56-61   : State Name (A6)
 *   62-63   : Offense Month (01-12)
 *   64-69   : Last Update (MMDDYY)
 *   70      : Action Type (0=normal, 1=adjustment)
 *   71      : Homicide type (A=Murder, B=Manslaughter)
 *   72-74   : Incident Number
 *   75      : Situation (A-F)
 *   76-77   : Victim-1 Age
 *   78      : Victim-1 Sex
 *   79      : Victim-1 Race
 *   80      : Victim-1 Ethnicity
 *   81-85   : Offender-1 (skipped)
 *   86-87   : Weapon code
 *   88-89   : Relationship code
 *   90-91   : Circumstances code
 *   92      : Sub-Circumstance
 *   93-95   : Additional Victim Count (N3)
 *   96-98   : Additional Offender Count (N3)
 *   99-148  : Additional victims 02-11 (5 chars each: age2 sex1 race1 eth1)
 *   149-268 : Additional offenders (skipped)
 */

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

// State code -> abbreviation lookup
const STATE_CODES = {
  '50': 'AK', '01': 'AL', '03': 'AR', '54': 'AS', '02': 'AZ',
  '04': 'CA', '05': 'CO', '06': 'CT', '52': 'CZ', '08': 'DC',
  '07': 'DE', '09': 'FL', '10': 'GA', '55': 'GU', '51': 'HI',
  '14': 'IA', '11': 'ID', '12': 'IL', '13': 'IN', '15': 'KS',
  '16': 'KY', '17': 'LA', '20': 'MA', '19': 'MD', '18': 'ME',
  '21': 'MI', '22': 'MN', '24': 'MO', '23': 'MS', '25': 'MT',
  '32': 'NC', '33': 'ND', '26': 'NE', '28': 'NH', '29': 'NJ',
  '30': 'NM', '27': 'NV', '31': 'NY', '34': 'OH', '35': 'OK',
  '36': 'OR', '37': 'PA', '53': 'PR', '38': 'RI', '39': 'SC',
  '40': 'SD', '41': 'TN', '42': 'TX', '43': 'UT', '62': 'VI',
  '45': 'VA', '44': 'VT', '46': 'WA', '48': 'WI', '47': 'WV',
  '49': 'WY'
};

const WEAPON_CODES = {
  '11': 'Firearm (type not stated)',
  '12': 'Handgun',
  '13': 'Rifle',
  '14': 'Shotgun',
  '15': 'Other Gun',
  '20': 'Knife/Cutting Instrument',
  '30': 'Blunt Object',
  '40': 'Personal Weapons',
  '50': 'Poison',
  '55': 'Pushed/Thrown Out Window',
  '60': 'Explosives',
  '65': 'Fire',
  '70': 'Narcotics/Drugs',
  '75': 'Drowning',
  '80': 'Strangulation/Hanging',
  '85': 'Asphyxiation',
  '90': 'Other/Unknown'
};

const WEAPON_GROUPS = {
  '11': 'Firearm', '12': 'Firearm', '13': 'Firearm', '14': 'Firearm', '15': 'Firearm',
  '20': 'Knife/Cutting', '30': 'Blunt Object', '40': 'Personal Weapons',
  '50': 'Poison', '55': 'Other', '60': 'Other', '65': 'Fire',
  '70': 'Narcotics/Drugs', '75': 'Other', '80': 'Strangulation', '85': 'Asphyxiation',
  '90': 'Other'
};

const RELATIONSHIP_CODES = {
  'HU': 'Husband', 'WI': 'Wife', 'CH': 'Common-Law Husband', 'CW': 'Common-Law Wife',
  'MO': 'Mother', 'FA': 'Father', 'SO': 'Son', 'DA': 'Daughter',
  'BR': 'Brother', 'SI': 'Sister', 'IL': 'In-Law',
  'SF': 'Stepfather', 'SM': 'Stepmother', 'SS': 'Stepson', 'SD': 'Stepdaughter',
  'OF': 'Other Family',
  'NE': 'Neighbor', 'AQ': 'Acquaintance', 'BF': 'Boyfriend', 'GF': 'Girlfriend',
  'XH': 'Ex-Husband', 'XW': 'Ex-Wife', 'EE': 'Employee', 'ER': 'Employer',
  'FR': 'Friend', 'HO': 'Homosexual Relationship', 'OK': 'Other Known',
  'ST': 'Stranger', 'UN': 'Unknown'
};

const RELATIONSHIP_GROUPS = {};
['HU','WI','CH','CW','MO','FA','SO','DA','BR','SI','IL','SF','SM','SS','SD','OF'].forEach(c => RELATIONSHIP_GROUPS[c] = 'Family');
['NE','AQ','BF','GF','XH','XW','EE','ER','FR','HO','OK'].forEach(c => RELATIONSHIP_GROUPS[c] = 'Known');
RELATIONSHIP_GROUPS['ST'] = 'Stranger';
RELATIONSHIP_GROUPS['UN'] = 'Unknown';

const CIRCUMSTANCE_CODES = {
  '02': 'Rape', '03': 'Robbery', '05': 'Burglary', '06': 'Larceny',
  '07': 'Motor Vehicle Theft', '09': 'Arson', '10': 'Prostitution',
  '17': 'Other Sex Offenses', '32': 'Abortion', '18': 'Narcotic Drug Laws',
  '19': 'Gambling', '26': 'Other Felony',
  '40': "Lover's Triangle", '41': 'Child Killed by Babysitter',
  '42': 'Brawl (Alcohol)', '43': 'Brawl (Narcotics)',
  '44': 'Argument Over Money/Property', '45': 'Other Arguments',
  '46': 'Gangland Killings', '47': 'Juvenile Gang Killings',
  '48': 'Institutional Killings', '49': 'Sniper Attack', '60': 'Other',
  '70': 'All Suspected Felony',
  '80': 'Felon Killed by Private Citizen', '81': 'Felon Killed by Police',
  '99': 'Unable to Determine',
  '50': 'Hunting Accident', '51': 'Gun-Cleaning Death',
  '52': 'Children Playing With Gun', '53': 'Other Negligent Gun Handling',
  '59': 'Other Manslaughter by Negligence'
};

const CIRCUMSTANCE_GROUPS = {};
['02','03','05','06','07','09','10','17','32','18','19','26','70'].forEach(c => CIRCUMSTANCE_GROUPS[c] = 'Felony Type');
['40','41','42','43','44','45','46','47','48','49','60'].forEach(c => CIRCUMSTANCE_GROUPS[c] = 'Other Than Felony');
['80','81'].forEach(c => CIRCUMSTANCE_GROUPS[c] = 'Justifiable Homicide');
CIRCUMSTANCE_GROUPS['99'] = 'Undetermined';
['50','51','52','53','59'].forEach(c => CIRCUMSTANCE_GROUPS[c] = 'Manslaughter by Negligence');

const RACE_CODES = { 'W': 'White', 'B': 'Black', 'I': 'American Indian', 'A': 'Asian/Pacific Islander', 'U': 'Unknown' };
const SEX_CODES = { 'M': 'Male', 'F': 'Female', 'U': 'Unknown' };
const ETHNICITY_CODES = { 'H': 'Hispanic', 'N': 'Not Hispanic', 'U': 'Unknown' };
const HOMICIDE_TYPES = { 'A': 'Murder', 'B': 'Manslaughter' };
const SITUATION_CODES = {
  'A': 'Single Victim/Single Offender', 'B': 'Single Victim/Unknown Offender',
  'C': 'Single Victim/Multiple Offenders', 'D': 'Multiple Victims/Single Offender',
  'E': 'Multiple Victims/Multiple Offenders', 'F': 'Multiple Victims/Unknown Offender'
};

function parseRecord(line) {
  if (line.length < 92) return null;
  if (line[0] !== '6') return null;

  const stateCode = line.substring(1, 3);
  const stateAbbr = STATE_CODES[stateCode];
  if (!stateAbbr) return null;

  const ori = line.substring(3, 10).trim();
  const group = line.substring(10, 12).trim();
  const division = line[12];
  const yearRaw = line.substring(13, 15);
  const population = parseInt(line.substring(15, 24)) || 0;
  const agency = line.substring(31, 55).trim();
  const stateName = line.substring(55, 61).trim();
  const month = parseInt(line.substring(61, 63)) || 0;
  const actionType = line[69];
  const homicideType = line[70];
  const incidentNum = line.substring(71, 74).trim();
  const situation = line[74];

  // Victim-1
  const v1Age = line.substring(75, 77).trim();
  const v1Sex = line[77];
  const v1Race = line[78];
  const v1Ethnicity = line[79];

  // Weapon, relationship, circumstance
  const weapon = line.substring(85, 87).trim();
  const relationship = line.substring(87, 89).trim();
  const circumstance = line.substring(89, 91).trim();
  const subCircumstance = line[91] || '';

  // Additional victim count
  const addlVictimCount = parseInt(line.substring(92, 95)) || 0;

  // Parse additional victims
  const additionalVictims = [];
  if (addlVictimCount > 0 && line.length >= 99) {
    for (let i = 0; i < Math.min(addlVictimCount, 10); i++) {
      const offset = 98 + (i * 5);
      if (offset + 5 > line.length) break;
      const chunk = line.substring(offset, offset + 5);
      if (chunk.trim()) {
        additionalVictims.push({
          age: chunk.substring(0, 2).trim(),
          sex: chunk[2],
          race: chunk[3],
          ethnicity: chunk[4]
        });
      }
    }
  }

  return {
    stateCode, stateAbbr, ori, group, division, yearRaw,
    population, agency, stateName, month, actionType,
    homicideType, incidentNum, situation,
    v1Age, v1Sex, v1Race, v1Ethnicity,
    weapon, relationship, circumstance, subCircumstance,
    addlVictimCount, additionalVictims
  };
}

function resolveYear(yearRaw, fileYear) {
  const yr2 = parseInt(yearRaw);
  if (isNaN(yr2)) return fileYear;
  // Use file year's century
  const century = Math.floor(fileYear / 100) * 100;
  let year = century + yr2;
  // Handle edge cases (e.g., file is 2000 but record says 99)
  if (year > fileYear + 1) year -= 100;
  return year;
}

async function main() {
  const dataDir = path.join(__dirname, '..', 'data');
  const dbPath = path.join(dataDir, 'shr.db');

  // Remove existing DB
  if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath);

  // Use better-sqlite3
  let Database;
  try {
    Database = require('better-sqlite3');
  } catch {
    console.log('Installing better-sqlite3...');
    execSync('npm install better-sqlite3', { cwd: path.join(__dirname, '..'), stdio: 'inherit' });
    Database = require('better-sqlite3');
  }

  const db = new Database(dbPath);
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = OFF');

  db.exec(`
    CREATE TABLE incidents (
      id INTEGER PRIMARY KEY,
      year INTEGER NOT NULL,
      month INTEGER,
      state TEXT NOT NULL,
      state_name TEXT,
      ori TEXT,
      agency TEXT,
      population INTEGER,
      division INTEGER,
      homicide_type TEXT,
      situation TEXT,
      weapon TEXT,
      weapon_group TEXT,
      relationship TEXT,
      relationship_group TEXT,
      circumstance TEXT,
      circumstance_group TEXT,
      sub_circumstance TEXT,
      action_type TEXT,
      incident_num TEXT
    );

    CREATE TABLE victims (
      id INTEGER PRIMARY KEY,
      incident_id INTEGER NOT NULL,
      victim_num INTEGER NOT NULL,
      age TEXT,
      age_num INTEGER,
      sex TEXT,
      race TEXT,
      ethnicity TEXT,
      FOREIGN KEY (incident_id) REFERENCES incidents(id)
    );
  `);

  const insertIncident = db.prepare(`
    INSERT INTO incidents (year, month, state, state_name, ori, agency, population,
      division, homicide_type, situation, weapon, weapon_group,
      relationship, relationship_group, circumstance, circumstance_group,
      sub_circumstance, action_type, incident_num)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const insertVictim = db.prepare(`
    INSERT INTO victims (incident_id, victim_num, age, age_num, sex, race, ethnicity)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `);

  function parseAge(ageStr) {
    if (!ageStr || ageStr === '00') return { age: 'Unknown', ageNum: null };
    if (ageStr === 'NB') return { age: 'Newborn', ageNum: 0 };
    if (ageStr === 'BB') return { age: 'Baby', ageNum: 0 };
    if (ageStr === '99') return { age: '99+', ageNum: 99 };
    const n = parseInt(ageStr);
    if (!isNaN(n) && n >= 1 && n <= 98) return { age: String(n), ageNum: n };
    return { age: 'Unknown', ageNum: null };
  }

  // Process all zip files
  const zipFiles = fs.readdirSync(dataDir)
    .filter(f => f.match(/^shr-\d{4}\.zip$/))
    .sort();

  let totalRecords = 0;
  let totalVictims = 0;
  let skipped = 0;

  for (const zipFile of zipFiles) {
    const fileYear = parseInt(zipFile.match(/(\d{4})/)[1]);
    const zipPath = path.join(dataDir, zipFile);

    // Extract to stdout using unzip
    let content;
    try {
      content = execSync(`unzip -p "${zipPath}"`, { maxBuffer: 50 * 1024 * 1024 }).toString('latin1');
    } catch (e) {
      console.error(`Failed to unzip ${zipFile}: ${e.message}`);
      continue;
    }

    const lines = content.split(/\r?\n/);
    let fileRecords = 0;
    let fileVictims = 0;

    const insertMany = db.transaction(() => {
      for (const line of lines) {
        if (line.length < 92) continue;
        const rec = parseRecord(line);
        if (!rec) { skipped++; continue; }

        const year = resolveYear(rec.yearRaw, fileYear);
        if (year < 1960 || year > 2030) { skipped++; continue; }

        const result = insertIncident.run(
          year, rec.month, rec.stateAbbr, rec.stateName, rec.ori, rec.agency,
          rec.population, parseInt(rec.division) || 0,
          HOMICIDE_TYPES[rec.homicideType] || rec.homicideType,
          SITUATION_CODES[rec.situation] || rec.situation,
          WEAPON_CODES[rec.weapon] || rec.weapon || 'Unknown',
          WEAPON_GROUPS[rec.weapon] || 'Other',
          RELATIONSHIP_CODES[rec.relationship] || rec.relationship || 'Unknown',
          RELATIONSHIP_GROUPS[rec.relationship] || 'Unknown',
          CIRCUMSTANCE_CODES[rec.circumstance] || rec.circumstance || 'Unknown',
          CIRCUMSTANCE_GROUPS[rec.circumstance] || 'Unknown',
          rec.subCircumstance, rec.actionType, rec.incidentNum
        );

        const incidentId = result.lastInsertRowid;

        // Insert victim 1
        const v1 = parseAge(rec.v1Age);
        insertVictim.run(incidentId, 1, v1.age,  v1.ageNum,
          SEX_CODES[rec.v1Sex] || 'Unknown',
          RACE_CODES[rec.v1Race] || 'Unknown',
          ETHNICITY_CODES[rec.v1Ethnicity] || 'Unknown');
        fileVictims++;

        // Insert additional victims
        for (let i = 0; i < rec.additionalVictims.length; i++) {
          const av = rec.additionalVictims[i];
          const va = parseAge(av.age);
          insertVictim.run(incidentId, i + 2, va.age, va.ageNum,
            SEX_CODES[av.sex] || 'Unknown',
            RACE_CODES[av.race] || 'Unknown',
            ETHNICITY_CODES[av.ethnicity] || 'Unknown');
          fileVictims++;
        }

        fileRecords++;
      }
    });

    insertMany();
    totalRecords += fileRecords;
    totalVictims += fileVictims;
    console.log(`${fileYear}: ${fileRecords.toLocaleString()} incidents, ${fileVictims.toLocaleString()} victims`);
  }

  // Create indexes
  console.log('\nCreating indexes...');
  db.exec(`
    CREATE INDEX idx_incidents_year ON incidents(year);
    CREATE INDEX idx_incidents_state ON incidents(state);
    CREATE INDEX idx_incidents_year_state ON incidents(year, state);
    CREATE INDEX idx_incidents_weapon_group ON incidents(weapon_group);
    CREATE INDEX idx_incidents_circumstance_group ON incidents(circumstance_group);
    CREATE INDEX idx_incidents_relationship_group ON incidents(relationship_group);
    CREATE INDEX idx_incidents_homicide_type ON incidents(homicide_type);
    CREATE INDEX idx_victims_incident ON victims(incident_id);
    CREATE INDEX idx_victims_race ON victims(race);
    CREATE INDEX idx_victims_sex ON victims(sex);
  `);

  // Checkpoint and close
  db.pragma('wal_checkpoint(TRUNCATE)');
  db.close();

  const dbSize = (fs.statSync(dbPath).size / 1024 / 1024).toFixed(1);
  console.log(`\nDone! ${totalRecords.toLocaleString()} incidents, ${totalVictims.toLocaleString()} victims, ${skipped} skipped`);
  console.log(`Database: ${dbPath} (${dbSize} MB)`);
}

main().catch(e => { console.error(e); process.exit(1); });
