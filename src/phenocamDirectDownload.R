NeonPhenoCamList <- c("NEON.D01.BART.DP1.00033","NEON.D01.BART.DP1.00042","NEON.D01.HARV.DP1.00033","NEON.D01.HARV.DP1.00042",
                      "NEON.D02.BLAN.DP1.00033","NEON.D02.BLAN.DP1.00042","NEON.D02.SCBI.DP1.00033","NEON.D02.SCBI.DP1.00042",
                      "NEON.D02.SERC.DP1.00033","NEON.D02.SERC.DP1.00042","NEON.D03.DSNY.DP1.00033","NEON.D03.DSNY.DP1.00042",
                      "NEON.D03.JERC.DP1.00033","NEON.D03.JERC.DP1.00042","NEON.D03.OSBS.DP1.00033","NEON.D03.OSBS.DP1.00042",
                      "NEON.D04.GUAN.DP1.00033","NEON.D04.GUAN.DP1.00042","NEON.D04.LAJA.DP1.00033","NEON.D04.LAJA.DP1.00042",
                      "NEON.D05.STEI.DP1.00033","NEON.D05.STEI.DP1.00042","NEON.D05.TREE.DP1.00033","NEON.D05.TREE.DP1.00042",
                      "NEON.D05.UNDE.DP1.00033","NEON.D05.UNDE.DP1.00042","NEON.D06.KONA.DP1.00033","NEON.D06.KONA.DP1.00042",
                      "NEON.D06.KONZ.DP1.00033","NEON.D06.KONZ.DP1.00042","NEON.D06.UKFS.DP1.00033","NEON.D06.UKFS.DP1.00042",
                      "NEON.D07.GRSM.DP1.00033","NEON.D07.GRSM.DP1.00042","NEON.D07.MLBS.DP1.00033","NEON.D07.MLBS.DP1.00042",
                      "NEON.D07.ORNL.DP1.00033","NEON.D07.ORNL.DP1.00042","NEON.D08.DELA.DP1.00033","NEON.D08.DELA.DP1.00042",
                      "NEON.D08.LENO.DP1.00033","NEON.D08.LENO.DP1.00042","NEON.D08.TALL.DP1.00033","NEON.D08.TALL.DP1.00042",
                      "NEON.D09.DCFS.DP1.00033","NEON.D09.DCFS.DP1.00042","NEON.D09.NOGP.DP1.00033","NEON.D09.NOGP.DP1.00042",
                      "NEON.D09.WOOD.DP1.00033","NEON.D09.WOOD.DP1.00042","NEON.D10.CPER.DP1.00033","NEON.D10.CPER.DP1.00042",
                      "NEON.D10.RMNP.DP1.00033",
                      "NEON.D10.RMNP.DP1.00042","NEON.D10.STER.DP1.00033","NEON.D10.STER.DP1.00042","NEON.D11.CLBJ.DP1.00033",
                      "NEON.D11.CLBJ.DP1.00042","NEON.D11.OAES.DP1.00033","NEON.D11.OAES.DP1.00042","NEON.D12.YELL.DP1.00042",
                      "NEON.D12.YELL.DP1.00042","NEON.D13.MOAB.DP1.00033","NEON.D13.MOAB.DP1.00042","NEON.D13.NIWO.DP1.00033",
                      "NEON.D13.NIWO.DP1.00042","NEON.D14.SRER.DP1.00033","NEON.D14.SRER.DP1.00042","NEON.D14.JORN.DP1.00033",
                      "NEON.D14.JORN.DP1.00042","NEON.D12.YELL.DP1.00033","NEON.D12.YELL.DP1.00042","NEON.D15.ONAQ.DP1.00033",
                      "NEON.D15.ONAQ.DP1.00042","NEON.D15.ONAQ.DP1.00033","NEON.D15.ONAQ.DP1.00042","NEON.D16.ABBY.DP1.00033",
                      "NEON.D16.ABBY.DP1.00042","NEON.D16.WREF.DP1.00033","NEON.D16.WREF.DP1.00042","NEON.D17.SOAP.DP1.00033",
                      "NEON.D17.SOAP.DP1.00042","NEON.D17.TEAK.DP1.00033","NEON.D17.TEAK.DP1.00042","NEON.D17.SJER.DP1.00033",
                      "NEON.D17.SJER.DP1.00042","NEON.D18.BARR.DP1.00033","NEON.D18.BARR.DP1.00042","NEON.D18.TOOL.DP1.00033",
                      "NEON.D18.TOOL.DP1.00042","NEON.D19.BONA.DP1.00033","NEON.D19.BONA.DP1.00042","NEON.D19.DEJU.DP1.00033",
                      "NEON.D19.DEJU.DP1.00042","NEON.D19.HEAL.DP1.00033","NEON.D19.HEAL.DP1.00042","NEON.D20.PUUM.DP1.00033",
                      "NEON.D20.PUUM.DP1.00042"
)

for(i in NeonPhenoCamList){
  download.file(url = paste0("https://phenocam.sr.unh.edu/data/latest/",i ,".jpg"),
                destfile = paste0("X:/1_Data/phenoCam/",i,".jpg"), mode = "wb")
}
