/*

1. get the latest user-agents library from pip and then get the ua-parser-up2date
mkdir tmp
pip wheel user-agents --no-cache-dir --wheel-dir tmp/ 
pip3 wheel ua-parser-up2date --no-cache-dir --wheel-dir tmp/
cd tmp
2. copy the python libraries to S3 so that they can be loaded into our redshift cluster

-- aws s3 cp ua_parser-0.10.0-py2.py3-none-any.whl s3://philo-ott-analytics/ua-parser.zip
aws s3 cp ua_parser_up2date-0.15.0-py2.py3-none-any.whl s3://philo-ott-analytics/ua-parser.zip
aws s3 cp user_agents-2.1-py3-none-any.whl s3://philo-ott-analytics/user-agents.zip

3. load the UA parser and user_agents into Redshift and create our parsing function

*/
CREATE OR REPLACE LIBRARY ua_parser LANGUAGE plpythonu
  FROM 's3://philo-ott-analytics/philo-ua-parse/ua_parser_plus.zip'
  iam_role 'arn:aws:iam::333164654266:role/philo-ott-analytics-redshift';

CREATE OR REPLACE LIBRARY user_agents LANGUAGE plpythonu
  FROM 's3://philo-ott-analytics/philo-ua-parse/user-agents.zip'
    iam_role 'arn:aws:iam::333164654266:role/philo-ott-analytics-redshift';



CREATE OR REPLACE FUNCTION f_parse_ua_as_json(ua varchar(512)) RETURNS varchar(1024)
    STABLE
    LANGUAGE plpythonu
AS
$$
  if ua is None or ua == '': return None
  from ua_parser import user_agent_parser; import json;
  ua = user_agent_parser.Parse(ua)
  return json.dumps(ua)
$$;