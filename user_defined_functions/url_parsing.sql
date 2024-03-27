CREATE OR REPLACE FUNCTION f_hostname(url VARCHAR(max))
RETURNS varchar(max)
IMMUTABLE AS $$
import urlparse
if url is None:
  return None
return urlparse.urlparse(url).hostname
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION f_urldecode(url VARCHAR(max))
RETURNS varchar(max)
IMMUTABLE AS $$
import urlparse
if url is None:
  return None
return urlparse.unquote(url)
$$ LANGUAGE plpythonu;