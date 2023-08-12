select count(*)
from part
where regexp_matches(p_type, 'BRASS$');

