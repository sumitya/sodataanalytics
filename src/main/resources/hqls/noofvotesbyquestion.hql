select col.answer_id as answer, col.score as score,col.question_id as question_id,col.owner.user_id as owner
 from default.answers_json_temp1 order by score desc