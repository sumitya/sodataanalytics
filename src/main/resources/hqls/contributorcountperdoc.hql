select topics.ContributorCount as ContributorCount, topics.DocTagId, topics.ViewCount,topics.Id,
doctags.Id,doctags.tag, doctags.title,doctags.topiccount from doctags_json_temp1 doctags
 JOIN topics_json_temp1 topics ON(topics.Id=doctags.id) ORDER BY  ContributorCount desc