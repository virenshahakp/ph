create function f_base64decode(a character varying) returns character varying
    stable
    language plpythonu
as
$$  import base64
  if a is None:
    return None

  try:
    decoded = base64.b64decode(a)
    return decoded
  except:
    return a$$;

create function f_base64encode(a character varying) returns character varying
    stable
    language plpythonu
as
$$  import base64
  if a is None:
    return None

  try:
    encoded = base64.b64encode(a)
    return encoded
  except:
    return a$$;