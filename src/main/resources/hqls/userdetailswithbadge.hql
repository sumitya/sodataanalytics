select id,
accountid,
displayname,
location,
reputation,
col._Id,
col._Name
 from
 badges_medium_xml_temp1
 JOIN
 (select
 distinct(id) AS id,
accountid,
displayname,
location,
reputation from user_view where id is not null)
 users
 ON (col._Id=users.id)
 ORDER BY reputation DESC