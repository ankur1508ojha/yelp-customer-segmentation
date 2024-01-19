#Custom SQL Query1 :
#Parse Elite Column:

SELECT 
  USER_ID,
  AVERAGE_STARS,
  COMPLIMENT_COOL,
  COMPLIMENT_CUTE,
  COMPLIMENT_FUNNY,
  COMPLIMENT_HOT,
  COMPLIMENT_LIST,
  COMPLIMENT_MORE,
  COMPLIMENT_NOTE, 
  COMPLIMENT_PHOTOS,
  COMPLIMENT_PLAIN,
  COMPLIMENT_PROFILE,
  COMPLIMENT_WRITER,
  COOL,
  ELITE,
  FANS,
  FUNNY,
  NAME,
  REVIEW_COUNT,
  USEFUL,
  YELPING_SINCE ,
  FIRST_SEEN ,
  LAST_SEEN ,
  DATE_DIFF,
  DIFFERENT_BUSINESS_COUNT,
  AVG_RATING,
  MIN_STARS ,
  MAX_STARS ,
  CATEGORY_MAP,
  FRIENDS_COUNT,
  VALUE::INTEGER AS ELITE_YEAR
FROM 
    "Data_PROJECT"."YELP"."USERS",
  LATERAL FLATTEN(input => SPLIT(ELITE, ',')) AS t
