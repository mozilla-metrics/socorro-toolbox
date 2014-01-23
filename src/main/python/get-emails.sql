SELECT COUNT(*), rui.email
FROM reports_clean rc
JOIN signatures s ON
  s.signature_id = rc.signature_id
JOIN reports_user_info rui ON
  rui.uuid = rc.uuid AND rui.date_processed = rc.date_processed
WHERE
  rc.date_processed BETWEEN '2013-12-01' AND '2014-01-24' AND
  rui.date_processed BETWEEN '2013-12-01' AND '2014-01-24' AND
  s.signature IN ('StrChrIA', 'StrCmpNIA', 'StrStrIA') AND
  rui.email IS NOT NULL
GROUP BY email
