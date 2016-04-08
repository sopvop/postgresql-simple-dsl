{ mkDerivation, base, bytestring, mtl, postgresql-libpq
, postgresql-simple, stdenv, text, time, transformers
, unordered-containers, vinyl
}:
mkDerivation {
  pname = "postgresql-simple-dsl";
  version = "0.5.0.0";
  src = ./.;
  libraryHaskellDepends = [
    base bytestring mtl postgresql-libpq postgresql-simple text time
    transformers unordered-containers vinyl
  ];
  homepage = "https://github.com/sopvop/postgresql-simple-dsl";
  description = "PostgreSQL EDSL built on top of postgresql-simple";
  license = stdenv.lib.licenses.bsd3;
}
