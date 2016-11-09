#!/usr/bin/env Rscript
jinnDb <- read.csv("results/insertTestJinnDb.csv", header=TRUE, sep=",")
nedb <- read.csv("results/insertTestNedb.csv", header=TRUE, sep=",")
tingoDb <- read.csv("results/insertTestTingoDb.csv", header=TRUE, sep=",")

t_domain <- range(0, jinnDb$time, nedb$time, tingoDb$time)

plot(jinnDb$time, jinnDb$item, type="l", col="red", xlim=t_domain, ann=FALSE)
lines(nedb$time, nedb$item, col="blue")
lines(tingoDb$time, tingoDb$item, col="green")

grid()
title(xlab="Execution Time (ms)")
title(ylab="# of Inserted Elements")

m_range <- range(0, jinnDb$memTotal, nedb$memTotal)

plot(jinnDb$time, jinnDb$memUsed, type="l", col="firebrick1", xlim=t_domain, ylim=m_range, ann=FALSE)
lines(jinnDb$time, jinnDb$memTotal, type="l", col="firebrick3")
lines(nedb$time, nedb$memUsed, col="dodgerblue1")
lines(nedb$time, nedb$memTotal, col="dodgerblue3")
lines(tingoDb$time, tingoDb$memUsed, col="springgreen1")
lines(tingoDb$time, tingoDb$memTotal, col="springgreen3")

grid()
title(xlab="Execution Time (ms)");
title(ylab="Memory (bytes)")
