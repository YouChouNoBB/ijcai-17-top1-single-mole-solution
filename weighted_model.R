library(data.table)
library(parallel)

Yhzf = fread("user_pay.txt", col.names = c("yh_id", "sj_id", "sj"))
Yhzf$rx = as.integer(as.Date(Yhzf$sj, "%Y-%m-%d") - as.Date("2016-11-01", "%Y-%m-%d"))
Yhzf$yr = (Yhzf$rx + 904) %% 7 + 1
Sjtz = fread("shop_info.txt", col.names = c("sj_id", "sm", "wzbh", "rjxf", "pf", "pls", "mddj", "yjplmc", "ejflmc", "sjflmc"), encoding = "UTF-8")
Tq = fread("weather.txt", col.names = c("sm", "rq", "tq"))
Tq$rx = as.integer(as.Date(Tq$rq, "%Y-%m-%d") - as.Date("2016-11-01", "%Y-%m-%d"))

Xl = Yhzf[, .(sjkll = length(yh_id)), .(sj_id, rx, yr)]

Yc = function(xla, sjtza, tqa, qsrx, fw, fwa = fw, r = 0, s = 0){
	Qdzy = function(x, qz = 1){
		if (length(x[!is.na(x)]) == 0)
			return(0)

		df = double(0)
		for (a in min(x, na.rm = T):max(x, na.rm = T))
			df = c(df, sum(abs((a - x) / (a + x)) * qz, na.rm = T))
	
		mean((min(x, na.rm = T):max(x, na.rm = T))[df == min(df)])
	}

	Jsjzxs = function(yy, y, qz, r, s, bc){
		df = double(0)
		for(a in seq(r, s, bc))
			df = c(df, sum(abs((yy * a - y) / (yy * a + y) * qz), na.rm = T))

		mean(seq(r, s, bc)[df == min(df)])
	}

	Qdyrqz = function(yr, yra, r){
		qz = rep(0, length(yra))
		if (yr %in% 1:4){
			qz[yra %in% 1:4] = 1
			qz[yra == 5] = 1
			qz[yra %in% 6:7] = 1 - 0.95 * r
		}
		if (yr == 5){
			qz[yra %in% 1:4] = 1 - 0.95 * r
			qz[yra == 5] = 1
			qz[yra %in% 6:7] = 1 - 0.95 * r
		}
		if (yr == 6){
			qz[yra %in% 1:5] = 1 - 0.95 * r
			qz[yra == 6] = 1
			qz[yra == 7] = 1 - 0.32 * r
		}
		if (yr == 7){
			qz[yra %in% 1:5] = 1 - 0.95 * r
			qz[yra == 6] = 1 - 0.32 * r
			qz[yra == 7] = 1
		}
		qz
	}

	xla = merge(xla, sjtza[, .(sj_id, sm)], by = "sj_id", all.x = T)
	xla = merge(xla, tqa[, .(rx = rx - qsrx, sm, tq)], by = c("rx", "sm"), all.x = T)
	xla$tq[is.na(xla$tq)] = 0 
	xla$sjkll = xla$sjkll / (1 + 0.005 * xla$tq)

	sjyr = data.table(
		sj_id = rep(sjtza$sj_id, each = 7)
		, yr = 1:7
	)

	sjyr = merge(sjyr, xla[rx %in% fwa, .(sjyrpjkll = sum(sjkll / abs(rx %/% 7) ** 0.2) / sum(1 / abs(rx %/% 7) ** 0.2)), .(sj_id, yr)], by = c("sj_id", "yr"), all.x = T)
	sjyr = merge(sjyr, xla[rx %in% fwa, .(sjpjkll = sum(sjkll / abs(rx %/% 7) ** 0.2) / sum(1 / abs(rx %/% 7) ** 0.2)), sj_id], by = "sj_id", all.x = T)
	sjyr$sjyrb = sjyr$sjyrpjkll / sjyr$sjpjkll
	sjyr$sjyrb[is.na(sjyr$sjyrb)] = 1
	sjyr = sjyr[, .(sj_id, yr, sjyrb)]

	sjyrjzxs = xla[rx %in% fwa, .(sj_id, rx, zx = rx %/% 7, yr, sjkll)]
	sjyrjzxs = merge(sjyrjzxs, xla[rx %in% fwa, .(sjzzykll = Qdzy(sjkll)), .(sj_id, zx = rx %/% 7)], by = c("sj_id", "zx"))
	sjyrjzxs = sjyrjzxs[, .(sjyrjzxs = Jsjzxs(sjzzykll, sjkll, 1 / abs(zx) ** 0.3, 0.3, 1.8, 0.01)), .(sj_id, yr)]

	xlb = xla[rx %in% fw, .(sj_id, rx, zx = rx %/% 7, yr, sjkll)]
	xlb = merge(xlb, xla[, .(zkll = sum(sjkll)), .(zx = rx %/% 7)], by = "zx", all.x = T)
	xlb$yczkll = sum(xla[rx >= -7]$sjkll)
	xlb$sjkll = xlb$sjkll / xlb$zkll * xlb$yczkll
	xlb[is.na(xlb)] = 0

	xlc = merge(xlb, xla[, .(sjzxrxa = min(rx)), sj_id], by = "sj_id")
	xlc = merge(xlc, xla[rx %in% fw, .(sjzxrxb = min(rx)), sj_id], by = "sj_id")
	xlc$sjzxrx = 0.5 * xlc$sjzxrxa + 0.5 * xlc$sjzxrxb

	xlc = merge(xlc, sjyrjzxs, by = c("sj_id", "yr"), all.x = T)
	xlc = merge(xlc, sjyr, by = c("sj_id", "yr"), all.x = T)
	xlc[is.na(xlc)] = 1
	xlc$sjkll = xlc$sjkll / (xlc$sjyrjzxs ** 0.3 * xlc$sjyrb ** 0.7) ** s

	ycxl = data.table(
		sj_id = rep(sjtza$sj_id, each = 14 * length(fw))
		, sm = rep(sjtza$sm, each = 14 * length(fw))
		, rx = rep(0:13, each = length(fw))
		, yr = rep((qsrx + 904:917) %% 7 + 1, each = length(fw))
		, xlrx = fw
		, xlyr = (fw + qsrx + 904) %% 7 + 1
	)
	ycxl = merge(ycxl, xlc[, .(sj_id, xlrx = rx, xlzx = zx, xlyr = yr, sjkll, sjzxrx)], by = c("sj_id", "xlrx", "xlyr"))
	ycxl[is.na(ycxl)] = 0
	ycxl = ycxl[, .(
		yc = 0.5 * Qdzy(sjkll, Qdyrqz(yr, xlyr, r) * abs(xlrx - sjzxrx))
		 + 0.3 * Qdzy(sjkll, (xlrx - min(fw) + 1) ** 3 * Qdyrqz(yr, xlyr, r) * abs(xlrx - sjzxrx))
		 + 0.2 * Qdzy(sjkll, 1 / abs(xlrx) * Qdyrqz(yr, xlyr, r))
	), .(sj_id, rx, yr, sjzxrx)]

	yca = data.table(
		sj_id = rep(sjtza$sj_id, each = 14)
		, rx = 0:13
		, yr = (qsrx + 904:917) %% 7 + 1
	)
	yca = merge(yca, ycxl, by = c("sj_id", "rx", "yr"), all.x = T)
	yca[is.na(yca)] = 0
	yca = merge(yca, sjyrjzxs, by = c("sj_id", "yr"), all.x = T)
	yca = merge(yca, sjyr, by = c("sj_id", "yr"), all.x = T)
	yca[is.na(yca)] = 1
	yca$yc = yca$yc * (yca$sjyrjzxs ** 0.3 * yca$sjyrb ** 0.7) ** s
	setorder(yca, rx)
	setorder(yca, sj_id)
	yca$yc
}

Xla = Xl
Qdyc = function(a, Yc, Xla, Sjtz, Tq){
	library(data.table)
	rj = c(-1:-21, -36:-42, -120:-133, -148:-175, -190:-203, -365:-378)
	if (a == 1)
		return(0.30 * Yc(Xla, Sjtz, Tq, 0, c(-1:-21, -36:-42), rj, r = 1))
	if (a == 2)
		return(0.10 * Yc(Xla, Sjtz, Tq, 0, c(-1:-21, -29:-42), rj, r = 1))
	if (a == 3)
		return(0.10 * Yc(Xla, Sjtz, Tq, 0, -1:-21, rj, r = 1))
	if (a == 4)
		return(0.10 * Yc(Xla, Sjtz, Tq, 0, -1:-28, rj, r = 1))
	if (a == 5)
		return(0.07 * Yc(Xla, Sjtz, Tq, 0, -1:-7, rj, s = 1))
	if (a == 6)
		return(0.09 * Yc(Xla, Sjtz, Tq, 0, -1:-14, rj, s = 1))
	if (a == 7)
		return(0.11 * Yc(Xla, Sjtz, Tq, 0, -1:-21, rj, s = 1))
	if (a == 8)
		return(0.13 * Yc(Xla, Sjtz, Tq, 0, c(-1:-21, -36:-42), rj, s = 1))
}

Cl = makeCluster(getOption("cl.cores", 4))
Ycl = parLapply(Cl, 1:8, Qdyc, Yc = Yc, Xla = Xla, Sjtz = Sjtz, Tq = Tq)
stopCluster(Cl)
Yca = data.table(sj_id = rep(Sjtz$sj_id, each = 14), rx = 0:13, yc = 0)
for (a in 1:8){
	Yca$yc = Yca$yc + Ycl[[a]]
}

Yca = merge(Yca, Sjtz[, .(sj_id, sm)], by = c("sj_id"))
Yca = merge(Yca, Tq[, .(rx = rx, sm, tq)], by = c("rx", "sm"), all.x = T)
Yca$tq[is.na(Yca$tq)] = 0
Yca$yc = Yca$yc * (1 + 0.005 * Yca$tq)

Yca$yc[Yca$rx == 10] = Yca$yc[Yca$rx == 10] * 1.1

Yca$yc = as.integer(Yca$yc)
setorder(Yca, rx)
setorder(Yca, sj_id)

Tj = data.table(sj_id = unique(Yca$sj_id))
for (a in 0:13){
	Tj = cbind(Tj, Yca[rx == a, .(yc)])
	names(Tj)[ncol(Tj)] = paste0("r", a)
}

write.table(Tj, "0.csv", col.names = F, row.names = F, sep = ",", quote = F)
