import {DoQuery} from './doquery.js'

const NEW_SEASON_IDENTIFIER = 14

;(async () => {
  try {
    const q0 = `
      SELECT *
      FROM teams_transitions
      WHERE new_season_id=?
    `
    const r0 = await DoQuery(q0, [NEW_SEASON_IDENTIFIER])
    let i = 0
    while (i < r0.length) {
      const oldTeamId = r0[i].old_team_id
      const newTeamId = r0[i].new_team_id
      const q1 = `
        SELECT *
        FROM players_teams
        WHERE team_id=?
      `
      const r1 = await DoQuery(q1, [oldTeamId])
      let j = 0
      while (j < r1.length) {
        const player = r1[j]
        const q2 = `
          INSERT INTO players_teams
          (team_id, player_id, team_role_id, active, season_id)
          VALUES(?, ?, ?, ?, ?)
        `
        const r2 = await DoQuery(q2, [newTeamId, player.player_id, player.team_role_id, player.active, NEW_SEASON_IDENTIFIER])
        j++
      }
      i++
    }
  } catch (e) {
    console.log(e)
  } finally {
    process.exit(1)
  }
})()
