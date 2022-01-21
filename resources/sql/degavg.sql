CREATE FUNCTION DEGAVG(sumsindir FLOAT, sumcosdir FLOAT)
RETURNS FLOAT DETERMINISTIC
RETURN IF(
  DEGREES(ATAN2(sumsindir, sumcosdir)) < 0,
  360 + DEGREES(ATAN2(sumsindir, sumcosdir)),
  DEGREES(ATAN2(sumsindir, sumcosdir))
)
