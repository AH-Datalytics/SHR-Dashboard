#!/usr/bin/env node
/**
 * Generate pre-aggregated JSON from the SHR SQLite database
 * for the single-page dashboard.
 */

const Database = require('better-sqlite3');
const fs = require('fs');
const path = require('path');

const dbPath = path.join(__dirname, '..', 'data', 'shr.db');
const outDir = path.join(__dirname, '..', 'data');
const db = new Database(dbPath, { readonly: true });

function query(sql) {
  return db.prepare(sql).all();
}

// 1. National trend by year
const byYear = query(`
  SELECT year, COUNT(*) as incidents,
    (SELECT COUNT(*) FROM victims v JOIN incidents i2 ON v.incident_id = i2.id WHERE i2.year = i.year) as victims
  FROM incidents i
  GROUP BY year ORDER BY year
`);

// 2. By state + year (only years, with state totals)
const byStateYear = query(`
  SELECT state, year, COUNT(*) as n
  FROM incidents GROUP BY state, year ORDER BY state, year
`);

// 3. By weapon group + year
const byWeaponYear = query(`
  SELECT weapon_group as g, year, COUNT(*) as n
  FROM incidents GROUP BY weapon_group, year ORDER BY weapon_group, year
`);

// 4. By weapon detail + year
const byWeaponDetailYear = query(`
  SELECT weapon as w, year, COUNT(*) as n
  FROM incidents GROUP BY weapon, year ORDER BY weapon, year
`);

// 5. By circumstance group + year
const byCircGroupYear = query(`
  SELECT circumstance_group as g, year, COUNT(*) as n
  FROM incidents GROUP BY circumstance_group, year ORDER BY circumstance_group, year
`);

// 6. By circumstance detail + year
const byCircDetailYear = query(`
  SELECT circumstance as c, year, COUNT(*) as n
  FROM incidents GROUP BY circumstance, year ORDER BY circumstance, year
`);

// 7. By relationship group + year
const byRelGroupYear = query(`
  SELECT relationship_group as g, year, COUNT(*) as n
  FROM incidents GROUP BY relationship_group, year ORDER BY relationship_group, year
`);

// 8. By relationship detail + year
const byRelDetailYear = query(`
  SELECT relationship as r, year, COUNT(*) as n
  FROM incidents GROUP BY relationship, year ORDER BY relationship, year
`);

// 9. By homicide type + year
const byHomicideType = query(`
  SELECT homicide_type as t, year, COUNT(*) as n
  FROM incidents GROUP BY homicide_type, year ORDER BY homicide_type, year
`);

// 10. By month + year
const byMonthYear = query(`
  SELECT month as m, year, COUNT(*) as n
  FROM incidents WHERE month BETWEEN 1 AND 12
  GROUP BY month, year ORDER BY month, year
`);

// 11. Victim sex + year
const byVictimSexYear = query(`
  SELECT v.sex as s, i.year, COUNT(*) as n
  FROM victims v JOIN incidents i ON v.incident_id = i.id
  GROUP BY v.sex, i.year ORDER BY v.sex, i.year
`);

// 12. Victim race + year
const byVictimRaceYear = query(`
  SELECT v.race as r, i.year, COUNT(*) as n
  FROM victims v JOIN incidents i ON v.incident_id = i.id
  GROUP BY v.race, i.year ORDER BY v.race, i.year
`);

// 13. Victim ethnicity + year
const byVictimEthYear = query(`
  SELECT v.ethnicity as e, i.year, COUNT(*) as n
  FROM victims v JOIN incidents i ON v.incident_id = i.id
  GROUP BY v.ethnicity, i.year ORDER BY v.ethnicity, i.year
`);

// 14. Victim age group + year
const byVictimAgeYear = query(`
  SELECT
    CASE
      WHEN v.age_num IS NULL THEN 'Unknown'
      WHEN v.age_num < 1 THEN 'Infant (<1)'
      WHEN v.age_num BETWEEN 1 AND 11 THEN 'Child (1-11)'
      WHEN v.age_num BETWEEN 12 AND 17 THEN 'Teen (12-17)'
      WHEN v.age_num BETWEEN 18 AND 24 THEN 'Young Adult (18-24)'
      WHEN v.age_num BETWEEN 25 AND 34 THEN '25-34'
      WHEN v.age_num BETWEEN 35 AND 44 THEN '35-44'
      WHEN v.age_num BETWEEN 45 AND 54 THEN '45-54'
      WHEN v.age_num BETWEEN 55 AND 64 THEN '55-64'
      ELSE '65+'
    END as ag,
    i.year, COUNT(*) as n
  FROM victims v JOIN incidents i ON v.incident_id = i.id
  GROUP BY ag, i.year ORDER BY ag, i.year
`);

// 15. By situation + year
const bySituationYear = query(`
  SELECT situation as s, year, COUNT(*) as n
  FROM incidents GROUP BY situation, year ORDER BY situation, year
`);

// 16. State-level totals for the map
const stateTotals = query(`
  SELECT state, COUNT(*) as total,
    MIN(year) as min_year, MAX(year) as max_year
  FROM incidents GROUP BY state ORDER BY total DESC
`);

// 17. Top agencies (with ORI for keying)
const topAgencies = query(`
  SELECT ori, agency, state, COUNT(*) as total
  FROM incidents
  GROUP BY ori
  ORDER BY total DESC
  LIMIT 200
`);

// 20. Agency list by state (all agencies with 10+ incidents)
const agencyList = query(`
  SELECT ori, agency, state, COUNT(*) as total
  FROM incidents
  GROUP BY ori
  HAVING total >= 10
  ORDER BY state, agency
`);

// 21. Per-agency by year (agencies with 50+ incidents)
const byAgencyYear = query(`
  SELECT ori, year, COUNT(*) as n
  FROM incidents
  WHERE ori IN (SELECT ori FROM incidents GROUP BY ori HAVING COUNT(*) >= 50)
  GROUP BY ori, year ORDER BY ori, year
`);

// 22. Per-agency weapon group (agencies with 50+ incidents)
const byAgencyWeapon = query(`
  SELECT ori, weapon_group as g, COUNT(*) as n
  FROM incidents
  WHERE ori IN (SELECT ori FROM incidents GROUP BY ori HAVING COUNT(*) >= 50)
  GROUP BY ori, weapon_group
`);

// 23. Firearm % of murders by year (murder only, not manslaughter)
const firearmMurderByYear = query(`
  SELECT year,
    SUM(CASE WHEN weapon_group = 'Firearm' THEN 1 ELSE 0 END) as firearm,
    COUNT(*) as total
  FROM incidents
  WHERE homicide_type = 'Murder'
  GROUP BY year ORDER BY year
`);

// 24. Firearm % of murders by state
const firearmMurderByState = query(`
  SELECT state,
    SUM(CASE WHEN weapon_group = 'Firearm' THEN 1 ELSE 0 END) as firearm,
    COUNT(*) as total
  FROM incidents
  WHERE homicide_type = 'Murder'
  GROUP BY state ORDER BY state
`);

// 25. Firearm % of murders by year + state
const firearmMurderByStateYear = query(`
  SELECT state, year,
    SUM(CASE WHEN weapon_group = 'Firearm' THEN 1 ELSE 0 END) as firearm,
    COUNT(*) as total
  FROM incidents
  WHERE homicide_type = 'Murder'
  GROUP BY state, year ORDER BY state, year
`);

// 26. Per-agency firearm % of murders
const firearmMurderByAgency = query(`
  SELECT ori,
    SUM(CASE WHEN weapon_group = 'Firearm' THEN 1 ELSE 0 END) as firearm,
    COUNT(*) as total
  FROM incidents
  WHERE homicide_type = 'Murder'
    AND ori IN (SELECT ori FROM incidents GROUP BY ori HAVING COUNT(*) >= 50)
  GROUP BY ori
`);

// 27. Per-agency firearm murder by year
const firearmMurderByAgencyYear = query(`
  SELECT ori, year,
    SUM(CASE WHEN weapon_group = 'Firearm' THEN 1 ELSE 0 END) as firearm,
    COUNT(*) as total
  FROM incidents
  WHERE homicide_type = 'Murder'
    AND ori IN (SELECT ori FROM incidents GROUP BY ori HAVING COUNT(*) >= 50)
  GROUP BY ori, year ORDER BY ori, year
`);

// 18. By state + weapon group (for state drill-down)
const byStateWeapon = query(`
  SELECT state, weapon_group as g, COUNT(*) as n
  FROM incidents GROUP BY state, weapon_group
`);

// 19. By state + victim race
const byStateVictimRace = query(`
  SELECT i.state, v.race as r, COUNT(*) as n
  FROM victims v JOIN incidents i ON v.incident_id = i.id
  GROUP BY i.state, v.race
`);

// 28. Victims by state + year
const byStateYearVictims = query(`
  SELECT i.state, i.year, COUNT(*) as n
  FROM victims v JOIN incidents i ON v.incident_id = i.id
  GROUP BY i.state, i.year ORDER BY i.state, i.year
`);

// 29. Victims by agency + year (agencies with 50+ incidents)
const byAgencyYearVictims = query(`
  SELECT i.ori, i.year, COUNT(*) as n
  FROM victims v JOIN incidents i ON v.incident_id = i.id
  WHERE i.ori IN (SELECT ori FROM incidents GROUP BY ori HAVING COUNT(*) >= 50)
  GROUP BY i.ori, i.year ORDER BY i.ori, i.year
`);

// 30. Victims by homicide_type + year
const byHomicideTypeVictims = query(`
  SELECT i.homicide_type as t, i.year, COUNT(*) as n
  FROM victims v JOIN incidents i ON v.incident_id = i.id
  GROUP BY i.homicide_type, i.year ORDER BY i.homicide_type, i.year
`);

// 31. Victims by month + year (national)
const byMonthYearVictims = query(`
  SELECT i.month as m, i.year, COUNT(*) as n
  FROM victims v JOIN incidents i ON v.incident_id = i.id
  WHERE i.month BETWEEN 1 AND 12
  GROUP BY i.month, i.year ORDER BY i.month, i.year
`);

// ── Population by year (distinct ORI populations) ──
const populationByYear = query(`
  SELECT year, SUM(pop) as total_pop, COUNT(*) as agencies
  FROM (
    SELECT year, ori, MAX(population) as pop
    FROM incidents
    WHERE population > 0
    GROUP BY year, ori
  )
  GROUP BY year ORDER BY year
`);

// 32. Consistent sample: agencies reporting 1+ incident every year 1986-2025
//     Expanded to include agencies missing ONLY 2021 (NIBRS transition) with inferred data
const perfectORIs = query(`
  SELECT ori FROM incidents
  WHERE year BETWEEN 1986 AND 2025
  GROUP BY ori
  HAVING COUNT(DISTINCT year) = 40
`).map(r => r.ori);

const inferred2021ORIs = query(`
  SELECT ori FROM incidents
  WHERE year BETWEEN 1986 AND 2025
  GROUP BY ori
  HAVING COUNT(DISTINCT year) = 39
    AND SUM(CASE WHEN year = 2021 THEN 1 ELSE 0 END) = 0
`).map(r => r.ori);

const allConsistentORIs = [...perfectORIs, ...inferred2021ORIs];
const oriList = allConsistentORIs.map(o => `'${o}'`).join(',');
const inferredOriList = inferred2021ORIs.length > 0 ? inferred2021ORIs.map(o => `'${o}'`).join(',') : "''";

// Helper: add inferred 2021 data to grouped query results
// For the 55 agencies missing 2021, average their 2020+2022 values
function infer2021(mainRows, sideRows, groupKey, valueKey = 'n') {
  if (inferred2021ORIs.length === 0) return mainRows;
  // Group side rows by (groupValue, year)
  const sideMap = {};
  for (const r of sideRows) {
    const g = groupKey ? r[groupKey] : '_all';
    if (!sideMap[g]) sideMap[g] = {};
    sideMap[g][r.year] = (sideMap[g][r.year] || 0) + r[valueKey];
  }
  // Compute inferred 2021 per group
  const inferred = {};
  for (const g of Object.keys(sideMap)) {
    const v2020 = sideMap[g][2020] || 0;
    const v2022 = sideMap[g][2022] || 0;
    inferred[g] = Math.round((v2020 + v2022) / 2);
  }
  // Add to 2021 rows
  for (const r of mainRows) {
    const g = groupKey ? r[groupKey] : '_all';
    if (r.year === 2021 && inferred[g]) {
      r[valueKey] += inferred[g];
    }
  }
  return mainRows;
}

// 33. Consistent sample: victims by year
const csByYear = query(`
  SELECT i.year,
    COUNT(DISTINCT i.id) as incidents,
    COUNT(*) as victims
  FROM victims v JOIN incidents i ON v.incident_id = i.id
  WHERE i.ori IN (${oriList})
  GROUP BY i.year ORDER BY i.year
`);
if (inferred2021ORIs.length > 0) {
  const inf = query(`
    SELECT i.year,
      COUNT(DISTINCT i.id) as incidents,
      COUNT(*) as victims
    FROM victims v JOIN incidents i ON v.incident_id = i.id
    WHERE i.ori IN (${inferredOriList}) AND i.year IN (2020, 2022)
    GROUP BY i.year
  `);
  infer2021(csByYear, inf, null, 'incidents');
  infer2021(csByYear, inf, null, 'victims');
}

// 33b. Consistent sample: victims by homicide type + year
const csByHomicideTypeYear = query(`
  SELECT i.homicide_type as t, i.year, COUNT(*) as n
  FROM victims v JOIN incidents i ON v.incident_id = i.id
  WHERE i.ori IN (${oriList})
  GROUP BY i.homicide_type, i.year ORDER BY i.homicide_type, i.year
`);
if (inferred2021ORIs.length > 0) {
  const inf = query(`
    SELECT i.homicide_type as t, i.year, COUNT(*) as n
    FROM victims v JOIN incidents i ON v.incident_id = i.id
    WHERE i.ori IN (${inferredOriList}) AND i.year IN (2020, 2022)
    GROUP BY i.homicide_type, i.year
  `);
  infer2021(csByHomicideTypeYear, inf, 't');
}

// 34. Consistent sample: victims by month + year
const csByMonthYear = query(`
  SELECT i.month as m, i.year, COUNT(*) as n
  FROM victims v JOIN incidents i ON v.incident_id = i.id
  WHERE i.ori IN (${oriList})
    AND i.month BETWEEN 1 AND 12
  GROUP BY i.month, i.year ORDER BY i.month, i.year
`);
if (inferred2021ORIs.length > 0) {
  const inf = query(`
    SELECT i.month as m, i.year, COUNT(*) as n
    FROM victims v JOIN incidents i ON v.incident_id = i.id
    WHERE i.ori IN (${inferredOriList}) AND i.year IN (2020, 2022)
      AND i.month BETWEEN 1 AND 12
    GROUP BY i.month, i.year
  `);
  infer2021(csByMonthYear, inf, 'm');
}

// 35. Consistent sample: firearm murder % by year
const csFirearmByYear = query(`
  SELECT year,
    SUM(CASE WHEN weapon_group = 'Firearm' THEN 1 ELSE 0 END) as firearm,
    COUNT(*) as total
  FROM incidents
  WHERE homicide_type = 'Murder'
    AND ori IN (${oriList})
  GROUP BY year ORDER BY year
`);
if (inferred2021ORIs.length > 0) {
  const inf = query(`
    SELECT year,
      SUM(CASE WHEN weapon_group = 'Firearm' THEN 1 ELSE 0 END) as firearm,
      COUNT(*) as total
    FROM incidents
    WHERE homicide_type = 'Murder'
      AND ori IN (${inferredOriList}) AND year IN (2020, 2022)
    GROUP BY year
  `);
  infer2021(csFirearmByYear, inf, null, 'firearm');
  infer2021(csFirearmByYear, inf, null, 'total');
}

// 36. Consistent sample: weapon group by year
const csByWeaponYear = query(`
  SELECT weapon_group as g, year, COUNT(*) as n
  FROM incidents
  WHERE ori IN (${oriList})
  GROUP BY weapon_group, year ORDER BY weapon_group, year
`);
if (inferred2021ORIs.length > 0) {
  const inf = query(`
    SELECT weapon_group as g, year, COUNT(*) as n
    FROM incidents
    WHERE ori IN (${inferredOriList}) AND year IN (2020, 2022)
    GROUP BY weapon_group, year
  `);
  infer2021(csByWeaponYear, inf, 'g');
}

// 37. Consistent sample: victim demographics by year
const csByVictimRaceYear = query(`
  SELECT v.race as r, i.year, COUNT(*) as n
  FROM victims v JOIN incidents i ON v.incident_id = i.id
  WHERE i.ori IN (${oriList})
  GROUP BY v.race, i.year ORDER BY v.race, i.year
`);
if (inferred2021ORIs.length > 0) {
  const inf = query(`
    SELECT v.race as r, i.year, COUNT(*) as n
    FROM victims v JOIN incidents i ON v.incident_id = i.id
    WHERE i.ori IN (${inferredOriList}) AND i.year IN (2020, 2022)
    GROUP BY v.race, i.year
  `);
  infer2021(csByVictimRaceYear, inf, 'r');
}

const csByVictimSexYear = query(`
  SELECT v.sex as s, i.year, COUNT(*) as n
  FROM victims v JOIN incidents i ON v.incident_id = i.id
  WHERE i.ori IN (${oriList})
  GROUP BY v.sex, i.year ORDER BY v.sex, i.year
`);
if (inferred2021ORIs.length > 0) {
  const inf = query(`
    SELECT v.sex as s, i.year, COUNT(*) as n
    FROM victims v JOIN incidents i ON v.incident_id = i.id
    WHERE i.ori IN (${inferredOriList}) AND i.year IN (2020, 2022)
    GROUP BY v.sex, i.year
  `);
  infer2021(csByVictimSexYear, inf, 's');
}

const csByVictimAgeYear = query(`
  SELECT
    CASE
      WHEN v.age_num IS NULL THEN 'Unknown'
      WHEN v.age_num < 1 THEN 'Infant (<1)'
      WHEN v.age_num BETWEEN 1 AND 11 THEN 'Child (1-11)'
      WHEN v.age_num BETWEEN 12 AND 17 THEN 'Teen (12-17)'
      WHEN v.age_num BETWEEN 18 AND 24 THEN 'Young Adult (18-24)'
      WHEN v.age_num BETWEEN 25 AND 34 THEN '25-34'
      WHEN v.age_num BETWEEN 35 AND 44 THEN '35-44'
      WHEN v.age_num BETWEEN 45 AND 54 THEN '45-54'
      WHEN v.age_num BETWEEN 55 AND 64 THEN '55-64'
      ELSE '65+'
    END as ag,
    i.year, COUNT(*) as n
  FROM victims v JOIN incidents i ON v.incident_id = i.id
  WHERE i.ori IN (${oriList})
  GROUP BY ag, i.year ORDER BY ag, i.year
`);
if (inferred2021ORIs.length > 0) {
  const inf = query(`
    SELECT
      CASE
        WHEN v.age_num IS NULL THEN 'Unknown'
        WHEN v.age_num < 1 THEN 'Infant (<1)'
        WHEN v.age_num BETWEEN 1 AND 11 THEN 'Child (1-11)'
        WHEN v.age_num BETWEEN 12 AND 17 THEN 'Teen (12-17)'
        WHEN v.age_num BETWEEN 18 AND 24 THEN 'Young Adult (18-24)'
        WHEN v.age_num BETWEEN 25 AND 34 THEN '25-34'
        WHEN v.age_num BETWEEN 35 AND 44 THEN '35-44'
        WHEN v.age_num BETWEEN 45 AND 54 THEN '45-54'
        WHEN v.age_num BETWEEN 55 AND 64 THEN '55-64'
        ELSE '65+'
      END as ag,
      i.year, COUNT(*) as n
    FROM victims v JOIN incidents i ON v.incident_id = i.id
    WHERE i.ori IN (${inferredOriList}) AND i.year IN (2020, 2022)
    GROUP BY ag, i.year
  `);
  infer2021(csByVictimAgeYear, inf, 'ag');
}

const csByCircGroupYear = query(`
  SELECT circumstance_group as g, year, COUNT(*) as n
  FROM incidents
  WHERE ori IN (${oriList})
  GROUP BY circumstance_group, year ORDER BY circumstance_group, year
`);
if (inferred2021ORIs.length > 0) {
  const inf = query(`
    SELECT circumstance_group as g, year, COUNT(*) as n
    FROM incidents
    WHERE ori IN (${inferredOriList}) AND year IN (2020, 2022)
    GROUP BY circumstance_group, year
  `);
  infer2021(csByCircGroupYear, inf, 'g');
}

const csByRelGroupYear = query(`
  SELECT relationship_group as g, year, COUNT(*) as n
  FROM incidents
  WHERE ori IN (${oriList})
  GROUP BY relationship_group, year ORDER BY relationship_group, year
`);
if (inferred2021ORIs.length > 0) {
  const inf = query(`
    SELECT relationship_group as g, year, COUNT(*) as n
    FROM incidents
    WHERE ori IN (${inferredOriList}) AND year IN (2020, 2022)
    GROUP BY relationship_group, year
  `);
  infer2021(csByRelGroupYear, inf, 'g');
}

// Consistent sample: population by year
const csPopByYear = query(`
  SELECT year, SUM(pop) as total_pop
  FROM (
    SELECT year, ori, MAX(population) as pop
    FROM incidents
    WHERE population > 0 AND ori IN (${oriList})
    GROUP BY year, ori
  )
  GROUP BY year ORDER BY year
`);
if (inferred2021ORIs.length > 0) {
  const inf = query(`
    SELECT year, SUM(pop) as total_pop
    FROM (
      SELECT year, ori, MAX(population) as pop
      FROM incidents
      WHERE population > 0 AND ori IN (${inferredOriList}) AND year IN (2020, 2022)
      GROUP BY year, ori
    )
    GROUP BY year
  `);
  infer2021(csPopByYear, inf, null, 'total_pop');
}

console.log(`Consistent sample: ${perfectORIs.length} perfect + ${inferred2021ORIs.length} inferred-2021 = ${allConsistentORIs.length} agencies`);

// Helper: pivot grouped data into { group: { year: count } }
function pivot(rows, groupKey, yearKey = 'year', countKey = 'n') {
  const result = {};
  for (const row of rows) {
    const g = row[groupKey];
    if (!result[g]) result[g] = {};
    result[g][row[yearKey]] = row[countKey];
  }
  return result;
}

const data = {
  years: byYear.map(r => r.year),
  byYear: byYear.map(r => ({ y: r.year, i: r.incidents, v: r.victims })),
  byState: pivot(byStateYear, 'state'),
  byWeaponGroup: pivot(byWeaponYear, 'g'),
  byWeaponDetail: pivot(byWeaponDetailYear, 'w'),
  byCircGroup: pivot(byCircGroupYear, 'g'),
  byCircDetail: pivot(byCircDetailYear, 'c'),
  byRelGroup: pivot(byRelGroupYear, 'g'),
  byRelDetail: pivot(byRelDetailYear, 'r'),
  byHomicideType: pivot(byHomicideType, 't'),
  byMonth: pivot(byMonthYear, 'm'),
  byVictimSex: pivot(byVictimSexYear, 's'),
  byVictimRace: pivot(byVictimRaceYear, 'r'),
  byVictimEthnicity: pivot(byVictimEthYear, 'e'),
  byVictimAge: pivot(byVictimAgeYear, 'ag'),
  bySituation: pivot(bySituationYear, 's'),
  stateTotals,
  topAgencies,
  agencyList: agencyList.map(r => ({ o: r.ori, a: r.agency, s: r.state, n: r.total })),
  byAgency: pivot(byAgencyYear, 'ori'),
  byAgencyWeapon: (() => {
    const r = {};
    for (const row of byAgencyWeapon) {
      if (!r[row.ori]) r[row.ori] = {};
      r[row.ori][row.g] = row.n;
    }
    return r;
  })(),
  firearmMurderPct: firearmMurderByYear.map(r => ({
    y: r.year, f: r.firearm, t: r.total, p: r.total ? +(r.firearm / r.total * 100).toFixed(1) : 0
  })),
  firearmMurderByState: (() => {
    const r = {};
    for (const row of firearmMurderByState) {
      r[row.state] = { f: row.firearm, t: row.total, p: row.total ? +(row.firearm / row.total * 100).toFixed(1) : 0 };
    }
    return r;
  })(),
  firearmMurderByStateYear: (() => {
    const r = {};
    for (const row of firearmMurderByStateYear) {
      if (!r[row.state]) r[row.state] = {};
      r[row.state][row.year] = { f: row.firearm, t: row.total };
    }
    return r;
  })(),
  firearmMurderByAgency: (() => {
    const r = {};
    for (const row of firearmMurderByAgency) {
      r[row.ori] = { f: row.firearm, t: row.total, p: row.total ? +(row.firearm / row.total * 100).toFixed(1) : 0 };
    }
    return r;
  })(),
  firearmMurderByAgencyYear: (() => {
    const r = {};
    for (const row of firearmMurderByAgencyYear) {
      if (!r[row.ori]) r[row.ori] = {};
      r[row.ori][row.year] = { f: row.firearm, t: row.total };
    }
    return r;
  })(),
  byStateWeapon: (() => {
    const r = {};
    for (const row of byStateWeapon) {
      if (!r[row.state]) r[row.state] = {};
      r[row.state][row.g] = row.n;
    }
    return r;
  })(),
  byStateVictimRace: (() => {
    const r = {};
    for (const row of byStateVictimRace) {
      if (!r[row.state]) r[row.state] = {};
      r[row.state][row.r] = row.n;
    }
    return r;
  })(),
  byStateVictims: pivot(byStateYearVictims, 'state'),
  byAgencyVictims: pivot(byAgencyYearVictims, 'ori'),
  byHomicideTypeVictims: pivot(byHomicideTypeVictims, 't'),
  byMonthVictims: pivot(byMonthYearVictims, 'm'),
  populationByYear: (() => {
    const r = {};
    for (const row of populationByYear) r[row.year] = row.total_pop;
    return r;
  })(),
  consistent: {
    count: allConsistentORIs.length,
    perfectCount: perfectORIs.length,
    inferred2021Count: inferred2021ORIs.length,
    byYear: csByYear.map(r => ({ y: r.year, i: r.incidents, v: r.victims })),
    byHomicideTypeVictims: pivot(csByHomicideTypeYear, 't'),
    populationByYear: (() => {
      const r = {};
      for (const row of csPopByYear) r[row.year] = row.total_pop;
      return r;
    })(),
    byMonth: pivot(csByMonthYear, 'm'),
    firearmPct: csFirearmByYear.map(r => ({
      y: r.year, f: r.firearm, t: r.total, p: r.total ? +(r.firearm / r.total * 100).toFixed(1) : 0
    })),
    byWeaponGroup: pivot(csByWeaponYear, 'g'),
    byVictimRace: pivot(csByVictimRaceYear, 'r'),
    byVictimSex: pivot(csByVictimSexYear, 's'),
    byVictimAge: pivot(csByVictimAgeYear, 'ag'),
    byCircGroup: pivot(csByCircGroupYear, 'g'),
    byRelGroup: pivot(csByRelGroupYear, 'g'),
  },
  meta: {
    totalIncidents: byYear.reduce((s, r) => s + r.incidents, 0),
    totalVictims: byYear.reduce((s, r) => s + r.victims, 0),
    minYear: byYear[0]?.year,
    maxYear: byYear[byYear.length - 1]?.year,
    generated: new Date().toISOString()
  }
};

const outPath = path.join(outDir, 'shr-aggregated.json');
fs.writeFileSync(outPath, JSON.stringify(data));
const size = (fs.statSync(outPath).size / 1024).toFixed(0);
console.log(`Written: ${outPath} (${size} KB)`);

db.close();
