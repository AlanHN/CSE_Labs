make
./fxmark/bin/fxmark --type=YFS --root=./native --ncore=1 --duration=5
./start.sh
./fxmark/bin/fxmark --type=YFS --root=./yfs1 --ncore=1 --duration=5
./stop.sh
make clean