#!/bin/bash

outdir="./record"
logdir="$outdir/twitch-log"
live_url="https://www.twitch.tv/chatnoir_ice"
prefix="twitch_chatnoir"

mkdir -p "$outdir" "$logdir"
while :
do
        logdate="$(date +%Y%m%d-%H%M%S)"
        logfile="$logdir/${prefix}_$1-${logdate}.ts.log"
        streamlink --retry-streams 30 --retry-max 120 --retry-open 3 --logfile "$logfile" --loglevel trace --hls-live-restart --output "$outdir/${prefix}_{author}_${logdate}_{time:%Y%m%d}_{time:%H%M%S}_{title}.ts" "$live_url" best
        ls "$outdir/${prefix}"*"${logdate}"*.ts 2>&1 >/dev/null
        if [ $? -eq 0 ]; then
                mv "$logfile" "$outdir"
        fi
        sleep 5
done
