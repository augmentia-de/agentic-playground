#!/bin/bash
#
# Erzeugt deps.tar.gz fuer den Docker-Build von quad-workflow-worker.
#
# Hintergrund
# ------------
# Das Dockerfile entpackt die Datei nach ~/.m2/repository. Alles, was es
# dort nicht findet, holt es anschliessend ueber `mvn dependency:resolve`
# aus Maven Central. Das Archiv muss deshalb genau die Artefakte enthalten,
# die es *nicht* oeffentlich gibt — also die lokal gebauten:
#
#     com.agentplatform:agent-common   (dieses Repo)
#     de.augmentia.quad:quad-core      (quad-quarkus)
#     de.augmentia.quad:quad-tool-builtin
#
# Dazu jeweils die Eltern-POMs, weil die Modul-POMs sie als <parent>
# deklarieren und Maven ohne sie die transitiven Abhaengigkeiten nicht
# aufloesen kann.
#
# Ein frueheres Archiv umfasste den kompletten de/augmentia-Baum (2,1 GB, inkl.
# quad-examples und worker) und war damit um faktor 3000 groesser als noetig.
#
# Verwendung
# ----------
#   1) agent-common bauen:   mvn -DskipTests install            (in diesem Repo)
#   2) quad-Frameworks:      cd ../quad-quarkus && mvn -DskipTests install
#   3) Tarball erzeugen:      ./scripts/build-deps-tarball.sh
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
M2_REPO="${M2_REPO:-$HOME/.m2/repository}"
TARGET="$REPO_ROOT/quad-workflow-worker/deps.tar.gz"

# groupId:artifactId -> Eltern-POM (groupId:artifactId)
declare -A LOCAL_ARTIFACTS=(
    ["com.agentplatform:agent-common"]="com.agentplatform:agent-platform"
    ["de.augmentia.quad:quad-core"]="de.augmentia.quad:quad"
    ["de.augmentia.quad:quad-tool-builtin"]="de.augmentia.quad:quad"
)

m2_path() {
    local group="$1" artifact="$2" version="$3"
    echo "$M2_REPO/${group//.//}/$artifact/$version"
}

# Veraltungswaechter: das Skript arbeitet auf ~/.m2, nicht auf dem Quellbaum.
# Ohne "mvn install" enthaelt das Archiv uralte Artefakte. Symptom im Docker-Build
# ist dann eine kryptische Kompilierfehlermeldung ("cannot find symbol ResultEvent"),
# weil das eingepackte agent-common aelter ist als der Quellcode. Deshalb wird
# eine Pflichtklasse geprueft, die es erst mit dem kanonischen Task/Result-Contract gibt.
require_class() {
    local jar="$1" class="$2" module="$3"
    # Hinweis: hier KEIN "unzip -l | grep -q". Unter "set -o pipefail" beendet
    # grep -q die Pipe sofort, unzip stirbt an SIGPIPE, und die Pipeline gilt
    # trotz Treffer als fehlgeschlagen. Deshalb erst die Liste einsammeln.
    local entries
    entries="$(unzip -Z1 "$jar" 2>/dev/null || true)"
    if ! printf '%s\n' "$entries" | grep -F "$class" > /dev/null; then
        echo "FEHLER: $module enthaelt die Klasse '$class' nicht."
        echo "       Das installierte Artefakt ist aelter als der Quellcode."
        echo "       Beheben mit:  mvn -DskipTests install   (im jeweiligen Projekt)"
        exit 1
    fi
}

STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

missing=0
for ga in "${!LOCAL_ARTIFACTS[@]}"; do
    group="${ga%%:*}"
    artifact="${ga##*:}"

    # Version aus dem lokalen Repo ermitteln (nur 1.0.0-SNAPSHOT im Einsatz).
    version=""
    if [[ -d "$M2_REPO/${group//.//}/$artifact" ]]; then
        version="$(ls "$M2_REPO/${group//.//}/$artifact" | head -1)"
    fi
    if [[ -z "$version" ]]; then
        echo "FEHLER: $ga nicht im Local-Repo gefunden."
        missing=1
        continue
    fi

    dir="$(m2_path "$group" "$artifact" "$version")"
    if ! compgen -G "$dir/*.jar" > /dev/null; then
        echo "FEHLER: kein JAR in $dir"
        missing=1
        continue
    fi
    mkdir -p "$STAGE/${group//.//}/$artifact/$version"
    cp -r "$dir"/* "$STAGE/${group//.//}/$artifact/$version/"
    # _remote.repositories und resolver-status.properties markieren Artefakte als
    # "aus Repository X bezogen". Maven verweigert sie dann im Container, weil X
    # dort nicht in der Repository-Liste steht — Symptom: "Could not find
    # artifact", obwohl die Datei im Archiv liegt. Beide sind fuer eine
    # Offline-Übernahme irrelevant.
    find "$STAGE/${group//.//}/$artifact" -name '_remote.repositories' -delete
    find "$STAGE/${group//.//}/$artifact" -name 'resolver-status.properties' -delete

    # Pflichtklassen je Modul — schlaegt Alarm, wenn das installierte JAR aelter ist.
    case "$ga" in
        com.agentplatform:agent-common)
            require_class "$STAGE/${group//.//}/$artifact/$version/agent-common-$version.jar" \
                "common/event/ResultEvent.class" "$ga" ;;
        de.augmentia.quad:quad-core)
            require_class "$STAGE/${group//.//}/$artifact/$version/quad-core-$version.jar" \
                "core/session/CurrentSession.class" "$ga" ;;
    esac

    parent="${LOCAL_ARTIFACTS[$ga]}"
    pgroup="${parent%%:*}"
    partifact="${parent##*:}"
    pversion="$(ls "$M2_REPO/${pgroup//.//}/$partifact" 2>/dev/null | head -1 || true)"
    if [[ -z "$pversion" ]]; then
        echo "FEHLER: Eltern-POM $parent fehlt (noetig fuer die Aufloesung transitiver Abhaengigkeiten)."
        missing=1
        continue
    fi
    pdir="$(m2_path "$pgroup" "$partifact" "$pversion")"
    mkdir -p "$STAGE/${pgroup//.//}/$partifact/$pversion"
    cp -r "$pdir"/* "$STAGE/${pgroup//.//}/$partifact/$pversion/"
    find "$STAGE/${pgroup//.//}/$partifact" -name '_remote.repositories' -delete
    find "$STAGE/${pgroup//.//}/$partifact" -name 'resolver-status.properties' -delete
    echo "  + $ga:$version (Eltern: $parent:$pversion)"
done

if [[ "$missing" != "0" ]]; then
    echo
    echo "Fehlende Artefakte zuerst bauen:"
    echo "  cd $REPO_ROOT && mvn -DskipTests install"
    echo "  cd $(dirname "$REPO_ROOT")/quad-quarkus && mvn -DskipTests install"
    exit 1
fi

mkdir -p "$(dirname "$TARGET")"
tar czf "$TARGET" -C "$STAGE" com de

SIZE="$(du -h "$TARGET" | cut -f1)"
echo "deps.tar.gz geschrieben: $TARGET ($SIZE)"
echo "Inhalt:"
tar tzf "$TARGET" | grep -E '\.(jar|pom)$' | sed 's/^/  /'
