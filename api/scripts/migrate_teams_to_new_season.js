import {DoQuery} from './doquery.js'

const NEW_SEASON_IDENTIFIER = 14
const OLD_SEASON_IDENTIFIER = 13

;(async () => {
  try {
    const q0 = `
      SELECT *
      FROM teams
      WHERE season_id=?
      AND status_id=1
    `
    const r0 = await DoQuery(q0, [OLD_SEASON_IDENTIFIER])
    console.log('records found: ', r0.length)

    let i = 0
    while (i < r0.length) {
      const q1 = `
        INSERT INTO teams
        (name, short_name, very_short_name, division_id, venue_id, status_id, line_groupid_team, advantage, fee_paid, new_team, top_team, season_id, is_test)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `
      const team = r0[i]
      const r1 = await DoQuery(q1, [team.name, team.short_name ?? '', team.very_short_name ?? '', 0, team.venue_id, 1, team.line_groupip_team ?? '', 0, 1, 0, 0, NEW_SEASON_IDENTIFIER, 0])
      const newTeamId = r1.insertId
      const q2 = `
        INSERT into teams_transitions
        (status_id, old_team_id, new_team_id, new_season_id)
        VALUES(?, ?, ?, ?)
      `
      const r2 = await DoQuery(q2, [0, team.id, newTeamId, NEW_SEASON_IDENTIFIER])
      i++
      console.log(i)
    }
  } catch (e) {
    console.log(e)
  } finally {
    process.exit(1)
  }
})()
