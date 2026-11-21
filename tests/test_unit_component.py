"""SICdb codes and their units, tested with suMEDS's shared `UnitComponentContract`.

No SICdb code ends in a unit: a code's unit is a literal (`"kg"`, `"min"`) or comes from a
dictionary table that records one unit per item (`d_references.ReferenceUnit`), never from a row. `expected_unit_events = ()` pins that, so a new code that
ends in a unit is a conscious change here. `require_unit_in_code` checks the other direction: every
event whose `_metadata._self.unit` reads a row column must give that unit in its code.
"""

from sumeds.testing.contracts import UnitComponentContract


class TestUnitComponents(UnitComponentContract):
    spec = "SICdb"
    expected_unit_events = ()
    require_unit_in_code = True
