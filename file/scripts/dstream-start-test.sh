#!/bin/bash
cd ../..
sbt "testOnly *BotDetectingDStreamTest -- -z bot"