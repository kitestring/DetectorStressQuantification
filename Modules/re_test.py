import re

stringPattern = "Voltage=.*AreaPerIon=.*"
compiled = re.compile(stringPattern)

fail = "At present voltage, area per ion is within tolerance of target. No change necessary."
success = 'Voltage=2045.400000 AreaPerIon=114.586685'

result1 = compiled.match(fail)
result2 = compiled.match(success)

print(result1)
print(result2)