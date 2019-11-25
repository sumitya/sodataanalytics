CREATE VIEW IF NOT EXISTS user_view AS select
  col._AccountId AS accountid
 ,col._DisplayName AS displayname
 ,col._Id AS id
 ,col._Location As location
 ,col._Reputation AS reputation
 FROM users_medium_xml_temp1