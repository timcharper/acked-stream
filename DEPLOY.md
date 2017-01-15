- bump version in `version.sbt`
- `sbt +test`
- `sbt +publishSigned`
- `sbt sonatypeRelease`

- commit tag and push

```
source project/version.properties
git add .
git commit -m "release $version"
git tag v$version
git push origin v$version


Temporarily set version:

`++2.10.5`
