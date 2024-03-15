create view ncaam_possession_vw as 
with game_data_cte as (
	select 
	game_id
	,date
	,home
	,away
	,row_number() over (partition by game_id order by half asc, (home_score+away_score) asc, play_id asc, secs_remaining desc) as play_id
	,secs_remaining
	,description 
	,action_team
	,case when action_team = 'away' then 
			case when lower(description) like '%turnover%' then away
			  when lower(description) like '%steal%' then home
			  when lower(description) like '%made%' then away 
			  when lower(description) like '%missed%' then away  
			  when lower(description) like '%offensive rebound%' then away
			  when lower(description) like '%defensive rebound%' then home
			  when lower(description) like '%block%' then home
			  else null end 
		else
			case when lower(description) like '%turnover%' then home
			  when lower(description) like '%steal%' then away
			  when lower(description) like '%made%' then home
			  when lower(description) like '%missed%' then home  
			  when lower(description) like '%offensive rebound%' then home
			  when lower(description) like '%defensive rebound%' then away
			  when lower(description) like '%block%' then away
			  else null end
		end as poss_team_obvious
	,home_score 
	,away_score
	,case when play_id = 1 then 2400 - secs_remaining else play_length end as play_length
	,case when lower(description) like '%turnover%' then 1 else 0 end as turnover
	,case when lower(description) like '%steal%' then 1 else 0 end as steal
	,case when lower(description) like '%made%' or lower(description) like '%missed%' then 1 else 0 end as shot_attempt
	,case when lower(description) like '%made%' then 1 else 0 end as made_shot
	,case when lower(description) like '%three point%' then 1 else 0 end as shot_3pt_flag
	,case when lower(description) like '%free throw%' then 1 else 0 end as free_throw_attempt
	,case when lower(description) like '%made free throw%' then 1 else 0 end as made_free_throw
	,case when lower(description) like '%block%' then 1 else 0 end as block 
	,case when lower(description) like '%offensive rebound%' then 1 else 0 end as offensive_rebound
	,case when lower(description) like '%defensive rebound%' then 1 else 0 end as defensive_rebound
from 
	ncaam_pbp
order by 
	secs_remaining desc)
	
,game_data_cte_v0 as 
	(select 
		game_id
		,date
		,home
		,away
		,play_id
		,secs_remaining
		,description 
		,action_team
		,poss_team_obvious
		,case when lower(description) like '%end of%' then lag(home_score) over (partition by game_id order by play_id)
	          when play_id = 1 and action_team = 'home' then 
	 				case when shot_3pt_flag = 1 and made_shot = 1 then 3.0 
	          			when made_free_throw = 1 then 1.0 
	 		  			when made_shot = 1 then 2.0 
	 		  			else 0.0 end 
		 	  else home_score end as home_score 
		,case when lower(description) like '%end of%' then lag(away_score) over (partition by game_id order by play_id)
	 		  when play_id = 1 and action_team = 'away' then 
	 				case when shot_3pt_flag = 1 and made_shot = 1 then 3.0 
	          			when made_free_throw = 1 then 1.0 
	 		  			when made_shot = 1 then 2.0 
	 		  			else 0.0 end 
		 	  else away_score end as away_score 
		,play_length
		,turnover
		,steal
		,shot_attempt
		,made_shot
		,shot_3pt_flag
		,free_throw_attempt
		,made_free_throw
		,block 
		,offensive_rebound
		,defensive_rebound
	from 
		game_data_cte
	order by 
		secs_remaining desc)

,game_data_cte_v1 as 
	(select 
		*
-- 	 	,case when description is null and home_score > lag(home_score) over (partition by game_id order by play_id) then home 
-- 	 		  when description is null and away_score > lag(away_score) over (partition by game_id order by play_id) then away
-- 	 		  else poss_team_obvious end as poss_team_obvious
	 	,home_score - coalesce(lag(home_score) over (partition by game_id order by play_id), 0) as home_points_play
		,away_score - coalesce(lag(away_score) over (partition by game_id order by play_id), 0) as away_points_play
	from 
		game_data_cte_v0) 

		
,game_data_cte_v2 as 
	(select 
		*
	from 
		game_data_cte_v1
	where 
		((action_team is not null or secs_remaining = 0) and
		--lower(description) not like '%timeout%' and 
		lower(description) not like '%deadball%') or 

		(description is null and home_points_play + away_points_play > 0)) 
	
,game_data_home_points as 
    (select 
        game_id 
        ,play_id 
        ,home_points_play 
    from 
        game_data_cte_v2
    where 
        home_points_play <> 0 
    )
,game_data_away_points as 
    (select 
        game_id
        ,play_id
        ,away_points_play 
    from 
        game_data_cte_v2
    where 
        away_points_play <> 0 )

,game_data_cte_v3 as 
    (select 
         a.game_id
		,a.date
		,a.home
		,a.away
		,a.play_id
		,a.secs_remaining
		,a.description 
		,a.action_team
		,a.poss_team_obvious
		,a.home_score 
		,a.away_score 
		,a.play_length
		,a.turnover
		,a.steal
		,a.shot_attempt
		,a.made_shot
		,a.shot_3pt_flag
		,a.free_throw_attempt
		,a.made_free_throw
		,a.block 
		,a.offensive_rebound
		,a.defensive_rebound
        ,b.home_points_play
        ,0 as away_points_play
    from 
        game_data_cte_v2 a 
    inner join 
        game_data_home_points b 
    on 
        a.game_id = b.game_id and 
        a.play_id = b.play_id
    union 
    select 
        a.game_id
		,a.date
		,a.home
		,a.away
		,a.play_id
		,a.secs_remaining
		,a.description 
		,a.action_team
		,a.poss_team_obvious
		,a.home_score 
		,a.away_score 
		,a.play_length
		,a.turnover
		,a.steal
		,a.shot_attempt
		,a.made_shot
		,a.shot_3pt_flag
		,a.free_throw_attempt
		,a.made_free_throw
		,a.block 
		,a.offensive_rebound
		,a.defensive_rebound
        ,0 as home_points_play
        ,b.away_points_play
    from 
        game_data_cte_v2 a 
    inner join 
        game_data_away_points b 
    on 
        a.game_id = b.game_id and 
        a.play_id = b.play_id
	union 
    select 
        a.game_id
		,a.date
		,a.home
		,a.away
		,a.play_id
		,a.secs_remaining
		,a.description 
		,a.action_team
		,a.poss_team_obvious
		,a.home_score 
		,a.away_score 
		,a.play_length
		,a.turnover
		,a.steal
		,a.shot_attempt
		,a.made_shot
		,a.shot_3pt_flag
		,a.free_throw_attempt
		,a.made_free_throw
		,a.block 
		,a.offensive_rebound
		,a.defensive_rebound
        ,0 as home_points_play
        ,0 as away_points_play
    from 
        game_data_cte_v2 a 
    left join 
        game_data_home_points b 
    on 
        a.game_id = b.game_id and 
        a.play_id = b.play_id
    left join
        game_data_away_points c
    on 
        a.game_id = c.game_id and 
        a.play_id = c.play_id 
    where 
        b.game_id is null and c.game_id is null)

,game_data_cte_v4 as 
(select 
	 *
	,case when home_points_play <> 0 then home 
 	      when away_points_play <> 0 then away 
--  		  when poss_team_obvious is null and home_points_play > 0 then home 
--  	      when poss_team_obvious is null and away_points_play > 0 then away 
          when poss_team_obvious is null and lag(poss_team_obvious) over (partition by game_id order by play_id) is null then lag(poss_team_obvious, 2) over (partition by game_id order by play_id)  
 		  when poss_team_obvious is null then lag(poss_team_obvious) over (partition by game_id order by play_id)
 		  else poss_team_obvious end as poss_team
from 
	game_data_cte_v3),
	
game_data_cte_v5 as
	(select
		game_id 
		,date
		,home
		,away
		,play_id
		,secs_remaining
		,description
		,poss_team
	 	,case when poss_team = away then home else away end as def_team
		,lag(poss_team_obvious) over (partition by game_id order by play_id) as lag_poss_team
	 	,lag(poss_team_obvious, 2) over (partition by game_id order by play_id) as lag2_poss_team
		,case when lag(poss_team) over (partition by game_id order by play_id) is null then 1 
			  when lag(poss_team) over (partition by game_id order by play_id) != poss_team then 1 
			  else 0 end as change_in_possession
		,home_score
		,away_score
		,play_length
		,turnover
		,steal
		,shot_attempt
		,made_shot
		,shot_3pt_flag
-- 	 	,case when shot_3pt_flag = 1 and made_shot = 1 then 3.0 
-- 	          when made_free_throw = 1 then 1.0 
-- 	 		  when made_shot = 1 then 2.0 
-- 	 		  else 0.0 end as points
	 	,(home_points_play) + (away_points_play) as points
		,free_throw_attempt 
		,made_free_throw
		,block
		,offensive_rebound
		,defensive_rebound
	from 
		game_data_cte_v4
	 order by play_id), 

game_data_cte_v6 as 
	(select 
		*
	 	,case when play_id = 1 then 2400 
	 		else lag(secs_remaining) over (partition by game_id order by play_id) end as starting_secs
		,sum(change_in_possession) over (partition by game_id order by play_id) as possession_id
	from 
		game_data_cte_v5), 

game_data_cte_v7 as 
	(select 
		game_id 
		,date 
		,home
		,away
		,possession_id
		,poss_team
	 	,def_team
		,case when poss_team = away then 'away' 
			  else 'home' end as poss_team_home_away
		,max(starting_secs) as possession_start_time 
		,min(secs_remaining) as possession_end_time 
		,max(home_score) as home_score
		,max(away_score) as away_score
		,sum(play_length) as play_length
		,sum(points)  as possession_points
		,sum(case when poss_team = home then points else 0 end) as home_poss_score 
		,sum(case when poss_team = away then points else 0 end) as away_poss_points
		,sum(turnover) as turnover
		,sum(steal) as steal 
		,sum(shot_attempt-free_throw_attempt) as shot_attempt
		,sum(made_shot-made_free_throw) as made_shot
		,sum(shot_3pt_flag) as three_point_shot
		,sum(free_throw_attempt) as free_throw_attempt 
		,sum(made_free_throw) as free_throw_made 
		,sum(block) as block 
		,sum(offensive_rebound) as offensive_rebound
		,sum(defensive_rebound) as defensive_rebound
	from 
		game_data_cte_v6
	group by 
		game_id 
		,date 
		,home
		,away
		,possession_id
		,poss_team
	 	,def_team
		,case when poss_team = away then 'away' 
			else 'home' end)
-- select 
-- 	* 
-- from 
-- 	game_data_cte_v4 
-- where 
-- 	game_id = 401573500
-- select 
-- 	game_id
-- 	,date
-- 	,home
-- 	,away
-- 	,possession_id
-- 	,poss_team
-- 	--,secs_remaining
-- 	,poss_team_home_away
-- 	,possession_end_time
-- 	,home_score
-- 	,home_poss_score
-- 	,sum(home_poss_score) over (partition by game_id order by possession_id) as home_score_calc
-- 	,away_score 
-- 	--,points
-- 	,sum(away_poss_points) over (partition by game_id order by possession_id) as away_score_calc
-- from 
-- 	game_data_cte_v6
-- where 
-- 	game_id = 401575402
-- select 
-- 	* 
-- 	,sum(home_poss_score) over (partition by game_id order by possession_id) as home_score_calc
-- 	,sum(away_poss_points) over (partition by game_id order by possession_id) as away_score_calc
-- from 
-- 	game_data_cte_v6 
-- where 
-- 	game_id = 401575402
,invalid_game_cte as 
	(select 
		game_id 
		,date
		,home
		,away
		,sum(case when poss_team = home then possession_points else 0 end) as calc_home_score 
		,sum(case when poss_team = home then 1 else 0 end) as calc_home_poss
		,max(home_score) as actual_home_score 
		,sum(case when poss_team = away then possession_points else 0 end) as calc_away_score
		,sum(case when poss_team = away then 1 else 0 end) as calc_away_poss
		,max(away_score) as actual_away_score
	from 
		game_data_cte_v7
	group by 
		game_id 
		,date
		,home
		,away
	having 
		sum(case when poss_team = home then possession_points else 0 end) <> max(home_score))

select 
	a.*
from 
	game_data_cte_v7 a
left join 
	invalid_game_cte b
on 
	a.game_id = b.game_id 
where 
	b.game_id is null
	