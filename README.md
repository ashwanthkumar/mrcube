mrcube
======

Scalding CUBE operators

Naive ``cubify`` and ``rollup`` methods on richPipe. 

For each Input Tuple

- cubify generates 2^n tuples, where n is the number of fields we are cubing on
- rollup generates n+1 tuples, where n is the number of fields we are rolling up on

