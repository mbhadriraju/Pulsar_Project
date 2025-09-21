# The PSRCAT2 Codebase

## Reading the Catalogue

psrcat2.py in development

## Updating the Catalogue

docs contains templates to product the update files for updating the catalogue

To commit to your version of the database, run as
```
./inputUpdate.tcsh -upd <updatefile> -commit 1
```
OR
If you wish to do a dry run, run as
```
./inputUpdate.tcsh -upd <updatefile> -commit 0
```
The default delimiter is ','. To use a different delimiter, run as

```
./inputUpdate.tcsh -upd <updatefile> -commit 1 -delim ';'
```