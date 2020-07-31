#!/bin/bash
cd ../..
sbt "testOnly *BotDetectingSStreamTest -- -z bot"