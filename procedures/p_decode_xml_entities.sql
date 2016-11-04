--
-- Copyright 2016 Roland Bouman, Just-Bi.nl
-- 
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
-- 
--     http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
drop PROCEDURE p_decode_xml_entities
;
--
-- Replaces xml entities into text.
-- This procedure is a helper of p_parse_xml.
--
-- It replaces xml entities passed into the p_encoded_text IN parameter with their text equivalents
-- and returns the result in the p_decoded_text OUT parameter.
--
-- The following types of entities are handled by this procedure:
-- - standard named entities: 
--     &amp;  becomes &
--     &apos; becomes '
--     &gt;   becomes >
--     &lt;   becomes <
--     &quot; becomes "
-- - decimal character references: 
--     &#65;  becomes A
-- - hexadecimal character references: 
--     &#x41; becomes A
--
-- Ideally, this should have been written as a scalar function 
-- that accepts one parameter and returns the result
-- but currently SAP/HANA does not allow nclob parameters.
--
create PROCEDURE p_decode_xml_entities ( 
  IN p_encoded_text nclob  -- text containing xml entities
, OUT p_decoded_text nclob -- text but with xml entities replaced by text 
) 
LANGUAGE SQLSCRIPT
SQL SECURITY INVOKER
READS SQL DATA
as 
BEGIN
  declare i integer default 1;
  declare n integer default length(p_encoded_text);
  declare v_from_position integer;
  declare v_to_position integer;
  declare v_text nclob default '';
  declare v_token nvarchar(12);
  
  while i <= n do
    select  locate_regexpr('&(amp|apos|lt|gt|quot|#(\d+|[xX]?[\dA-Za-z]+));' in p_encoded_text from i)
    into    v_from_position
    from    dummy
    ;
    if v_from_position = 0 then
      v_text = v_text || substr(p_encoded_text, i);
      i = n + 1;
    else
      v_text = v_text || substr(p_encoded_text, i, v_from_position - i);
      v_to_position = locate(p_encoded_text, ';', i);
      v_token = substr(p_encoded_text, v_from_position + 1, v_to_position - v_from_position - 1);
      if substr(v_token, 1, 1) = '#' then
        if substr(v_token, 2, 1) = 'x' then 
          v_text = v_text || bintostr(hextobin(substr(v_token, 3)));
        else
          v_text = v_text || nchar(cast(substr(v_token, 2) as integer));
        end if; 
      elseif v_token = 'amp' then
        v_text = v_text || '&';
      elseif v_token = 'apos' then
        v_text = v_text || '''';
      elseif v_token = 'lt' then
        v_text = v_text || '<';
      elseif v_token = 'gt' then
        v_text = v_text || '>';
      elseif v_token = 'quot' then
        v_text = v_text || '"';
      else
        signal sql_error_code 10000
        set message_text = 'Unrecognized entity '||v_token;
      end if;
      i = v_to_position + 1;
    end if
    ; 
  end while
  ;
  p_decoded_text = v_text;
END;
