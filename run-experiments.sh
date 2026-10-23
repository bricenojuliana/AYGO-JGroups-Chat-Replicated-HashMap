#!/usr/bin/env bash
# Runs the four required experiments unattended and saves each node's console in evidence/.
# Every node reads its console input from evidence/<exp>/in/<Node> (a file we append commands to).
# Usage: mvn package && ./run-experiments.sh
set -u
cd "$(dirname "$0")"
CP="target/classes:target/lib/*"
JAVA_OPTS="-Djava.net.preferIPv4Stack=true"

start() {   # start <node> [logSuffix]
  local n=$1 suf=${2:-}
  : > "$DIR/in/$n"
  (tail -f "$DIR/in/$n" | java $JAVA_OPTS -Dwal.dir="$DIR/wal" -cp "$CP" workshop.SimpleChat "$n" \
      > "$DIR/$n$suf.log" 2>&1 &)
  sleep 4
}
send()  { echo "$2" >> "$DIR/in/$1"; sleep 1.5; }
crash() { pkill -9 -f "workshop.SimpleChat $1\$"; sleep 1; }       # kill -9 = crash, no goodbye
quit()  { send "$1" "/quit"; pkill -f "tail -f $DIR/in/$1" ; sleep 1; }
newexp() { DIR="evidence/$1"; rm -rf "$DIR"; mkdir -p "$DIR/in" "$DIR/wal"; echo "== $1"; }
stopall() { for n in "$@"; do quit "$n"; done; pkill -f "tail -f $DIR/in" ; pkill -f workshop.SimpleChat; sleep 1; }

# ---------------------------------------------------------------- 1. normal operation
newexp 1-normal-operation
start Virginia; start London; start Bogota
send Virginia "Hola desde Virginia"
send London   "Hello from London"
send Bogota   "Saludos desde Bogota"
send Virginia "/store color azul"
send London   "/store ciudad Londres"
send Bogota   "/store curso AYGO"
send London   "/store color verde"
for n in Virginia London Bogota; do send $n "/state"; done
stopall Virginia London Bogota

# ---------------------------------------------------------------- 2. join + state transfer
newexp 2-join-state-transfer
start Virginia; start London
send Virginia "chat antes de que llegue Bogota"
send Virginia "/store a 1"
send London   "/store b 2"
send Virginia "/store c 3"
send London   "/store a 10"
start Bogota                                  # joins late -> state transfer
send Bogota   "/state"
send Bogota   "/store d 4"                    # new node participates in later updates
for n in Virginia London Bogota; do send $n "/state"; done
stopall Virginia London Bogota

# ---------------------------------------------------------------- 3. node failure  +  4. recovery from WAL
newexp 3-4-failure-and-recovery
start Virginia; start London; start Bogota
send Virginia "/store x 1"
send London   "/store y 2"
send Bogota   "/store z 3"
for n in Virginia London Bogota; do send $n "/state"; done
crash Bogota                                  # 3. failure: Bogota crashes (kill -9)
sleep 6                                       # failure detection -> new view on survivors
send Virginia "/store x 100"                  # cluster keeps working without Bogota
send London   "/store w 4"
send Virginia "/state"; send London "/state"
cp "$DIR/wal/Bogota.wal" "$DIR/Bogota.wal.before-restart"
start Bogota -restart                         # 4. recovery: replay WAL, rejoin, state transfer
send Bogota   "/state"
send Bogota   "/store v 5"
for n in Virginia London Bogota; do send $n "/state"; done
stopall Virginia London Bogota

echo "done -> evidence/"
