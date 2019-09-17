Test Cases
==========

We can end up with many gigs of test cases. To keep from blowing up the git
repository with these large binary files, any ROOT files larger than a couple
kB live in `testdata/pristine`. We do, however, need a way to track all these
files in a sensible way. Files in the manifest can be downloaded by executing
`scripts/get-pristine`.

We store the pristine manifest in `testdata/pristine-manifest.txt`. Any
accesses to pristine files that aren't in the manifest will cause a fatal error
in the accompanying test. This will serve as a reminder to unit test writers to
add tests pristine path and note them in the manifest.

Conversely, the test harness is instrumented to write out the files missing
from the unit tests to a temporary file named `testdata/pristine-missing.txt`.


Adding new cases
----------------

This isn't quite automated yet, so send the file to @PerilousApricot and make
sure to add the file to the pristine manifest
