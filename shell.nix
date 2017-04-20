{ pkgs ? import <nixpkgs> {}
, venvdir ? ".venv.stignore"
}:

with pkgs;

let

  py = pythonFull.withPackages (ps: with ps; [
    pip
    virtualenv
  ]);

in

stdenv.mkDerivation rec {
  name = "nist-fingerprint-example";
  buildInputs = [ py ];
  shellHook = ''
  export SOURCE_DATE_EPOCH=$(date +%s)
  export SSL_CERT_FILE=${cacert}/etc/ssl/certs/ca-bundle.crt
  test -d ${venvdir} || virtualenv --prompt '(${name}) ' ${venvdir}
  source ${venvdir}/bin/activate
  pip install -U -r big-data-stack/requirements.txt
  '';
}
