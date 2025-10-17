library(tidyverse)
library(janitor)
library(readxl)
options(scipen = 999)

xg2021 <- read_excel('/mnt/c/Users/Administrator/Downloads/2021-2024年实际结算新冠疫苗费及接种剂次统计表（分市州）.xls',sheet = 1,skip = 1)
xg2022 <- read_excel('/mnt/c/Users/Administrator/Downloads/2021-2024年实际结算新冠疫苗费及接种剂次统计表（分市州）.xls',sheet = 3,skip = 1)
xg2023 <- read_excel('/mnt/c/Users/Administrator/Downloads/2021-2024年实际结算新冠疫苗费及接种剂次统计表（分市州）.xls',sheet = 4,skip = 1)
xg2024 <- read_excel('/mnt/c/Users/Administrator/Downloads/2021-2024年实际结算新冠疫苗费及接种剂次统计表（分市州）.xls',sheet = 5,skip = 1)

xg2021z <- read_excel('/mnt/c/Users/Administrator/Downloads/2021-2024年实际结算新冠疫苗费及接种剂次统计表（分市州）(1).xls',sheet = 1,skip = 1)
xg2021s <- read_excel('/mnt/c/Users/Administrator/Downloads/2021-2024年实际结算新冠疫苗费及接种剂次统计表（分市州）(1).xls',sheet = 2,skip = 1)

xg2021 |> 
  mutate(
    剂次 = rowSums(across(starts_with("接种剂次")), na.rm = TRUE),
    费用 = rowSums(across(starts_with("疫苗费用")), na.rm = TRUE)
  ) |>
  select(shi=`...1`,剂次,费用)->tj2021

xg2022 |> 
  mutate(
    剂次 = rowSums(across(starts_with("接种剂次")), na.rm = TRUE),
    费用 = rowSums(across(starts_with("疫苗费用")), na.rm = TRUE)
  ) |>
  select(shi=`...1`,剂次,费用)->tj2022

xg2023 |> 
  mutate(
    剂次 = rowSums(across(starts_with("接种剂次")), na.rm = TRUE),
    费用 = rowSums(across(starts_with("疫苗费用")), na.rm = TRUE)
  ) |>
  select(shi=`...1`,剂次,费用)->tj2023

xg2024 |> 
  mutate(
    剂次 = rowSums(across(starts_with("接种剂次")), na.rm = TRUE),
    费用 = rowSums(across(starts_with("疫苗费用")), na.rm = TRUE)
  ) |>
  select(shi=`...1`,剂次,费用)->tj2024

xg2021z |> 
  mutate(
    剂次 = rowSums(across(starts_with("接种剂次")), na.rm = TRUE),
    费用 = rowSums(across(starts_with("疫苗费用")), na.rm = TRUE)
  ) |>
  select(shi=`...1`,剂次,费用)->tj2021z

xg2021s |> 
  mutate(
    剂次 = rowSums(across(starts_with("接种剂次")), na.rm = TRUE),
    费用 = rowSums(across(starts_with("疫苗费用")), na.rm = TRUE)
  ) |>
  select(shi=`...1`,剂次,费用)->tj2021s

tj2021 |> 
  left_join(tj2022,by=('shi'='shi')) |> 
  left_join(tj2023,by=('shi'='shi')) |> 
  left_join(tj2024,by=('shi'='shi')) |> 
  left_join(tj2021z,by=('shi'='shi')) |> 
  left_join(tj2021s,by=('shi'='shi')) -> tj

tj |> 
  mutate(剂次=`剂次.x`+`剂次.y`+`剂次.x.x`+`剂次.y.y`+`剂次.x.x.x`-`剂次.y.y.y`) |> 
  mutate(费用=`费用.x`+`费用.y`+`费用.x.x`+`费用.y.y`+`费用.x.x.x`-`费用.y.y.y`) |> 
  select(shi,剂次,费用) |> 
  openxlsx::write.xlsx('/mnt/c/Users/Administrator/Downloads/合并统计.xlsx')


############################

xg2021 <- read_excel('/mnt/c/Users/Administrator/Downloads/2021-2024年实际结算新冠疫苗费及接种剂次统计表（分市州） - JIK.xls',sheet = 1,skip = 1)
xg2022 <- read_excel('/mnt/c/Users/Administrator/Downloads/2021-2024年实际结算新冠疫苗费及接种剂次统计表（分市州） - JIK.xls',sheet = 2,skip = 1)
xg2023 <- read_excel('/mnt/c/Users/Administrator/Downloads/2021-2024年实际结算新冠疫苗费及接种剂次统计表（分市州） - JIK.xls',sheet = 3,skip = 1)
xg2024 <- read_excel('/mnt/c/Users/Administrator/Downloads/2021-2024年实际结算新冠疫苗费及接种剂次统计表（分市州） - JIK.xls',sheet = 4,skip = 1)

xg2021z <- read_excel('/mnt/c/Users/Administrator/Downloads/2021-2024年实际结算新冠疫苗费及接种剂次统计表（分市州） - JIK - 副本.xls',sheet = 1,skip = 1)
xg2021s <- read_excel('/mnt/c/Users/Administrator/Downloads/2021-2024年实际结算新冠疫苗费及接种剂次统计表（分市州） - JIK - 副本.xls',sheet = 2,skip = 1)

xg2021 |> 
  mutate(
    剂次 = rowSums(across(starts_with("接种剂次")), na.rm = TRUE),
    费用 = rowSums(across(starts_with("疫苗费用")), na.rm = TRUE)
  ) |>
  select(shi=`...1`,剂次,费用)->tj2021

xg2022 |> 
  mutate(
    剂次 = rowSums(across(starts_with("接种剂次")), na.rm = TRUE),
    费用 = rowSums(across(starts_with("疫苗费用")), na.rm = TRUE)
  ) |>
  select(shi=`...1`,剂次,费用)->tj2022

xg2023 |> 
  mutate(
    剂次 = rowSums(across(starts_with("接种剂次")), na.rm = TRUE),
    费用 = rowSums(across(starts_with("疫苗费用")), na.rm = TRUE)
  ) |>
  select(shi=`...1`,剂次,费用)->tj2023

xg2024 |> 
  mutate(
    剂次 = rowSums(across(starts_with("接种剂次")), na.rm = TRUE),
    费用 = rowSums(across(starts_with("疫苗费用")), na.rm = TRUE)
  ) |>
  select(shi=`...1`,剂次,费用)->tj2024

xg2021z |> 
  mutate(
    剂次 = rowSums(across(starts_with("接种剂次")), na.rm = TRUE),
    费用 = rowSums(across(starts_with("疫苗费用")), na.rm = TRUE)
  ) |>
  select(shi=`...1`,剂次,费用)->tj2021z

xg2021s |> 
  mutate(
    剂次 = rowSums(across(starts_with("接种剂次")), na.rm = TRUE),
    费用 = rowSums(across(starts_with("疫苗费用")), na.rm = TRUE)
  ) |>
  select(shi=`...1`,剂次,费用)->tj2021s

tj2021 |> 
  left_join(tj2022,by=('shi'='shi')) |> 
  left_join(tj2023,by=('shi'='shi')) |> 
  left_join(tj2024,by=('shi'='shi')) |> 
  left_join(tj2021z,by=('shi'='shi')) |> 
  left_join(tj2021s,by=('shi'='shi')) -> tj

tj |> 
  mutate(剂次=`剂次.x`+`剂次.y`+`剂次.x.x`+`剂次.y.y`+`剂次.x.x.x`-`剂次.y.y.y`) |> 
  mutate(费用=`费用.x`+`费用.y`+`费用.x.x`+`费用.y.y`+`费用.x.x.x`-`费用.y.y.y`) |> 
  select(shi,剂次,费用) |> 
  openxlsx::write.xlsx('/mnt/c/Users/Administrator/Downloads/合并统计2.xlsx')


###############
jg <- readxl::read_excel('/mnt/c/Users/Administrator/Downloads/3-12月新增/疫苗批号价格.xlsx') |> 
  select(疫苗批号,价格) |> 
  distinct(.keep_all = T)

read.csv('/mnt/c/Users/Administrator/Downloads/3-12月新增/2021年3-12月增减.CSV格式/新增数据/6新增.csv', fileEncoding = "GBK") |> 
  left_join(jg,by=c('ym_ph'='疫苗批号')) |> 
  mutate(shi = case_when(
    shi == 6232 ~ 6201,
    TRUE ~ shi
  )) |> 
  group_by(shi) |> 
  summarise(剂次数=n(),费用=sum(价格)) |> 
  janitor::adorn_totals()|> 
  openxlsx::write.xlsx('/mnt/c/Users/Administrator/Downloads/6月新增.xlsx')

read.csv('/mnt/c/Users/Administrator/Downloads/3-12月新增/2021年3-12月增减.CSV格式/删除数据(1)/5.csv', fileEncoding = "GBK") |> 
  left_join(jg,by=c('ym_ph'='疫苗批号')) |> 
  mutate(shi = case_when(
    shi == 6232 ~ 6201,
    TRUE ~ shi
  )) |> 
  group_by(shi) |> 
  summarise(剂次数=n(),费用=sum(价格)) |> 
  janitor::adorn_totals()|> 
  openxlsx::write.xlsx('/mnt/c/Users/Administrator/Downloads/5月删除.xlsx')

read.csv('/mnt/c/Users/Administrator/Downloads/3-12月新增/2021年3-12月增减.CSV格式/删除数据(1)/6.csv', fileEncoding = "GBK") |> 
  left_join(jg,by=c('ym_ph'='疫苗批号')) |> 
  mutate(shi = case_when(
    shi == 6232 ~ 6201,
    TRUE ~ shi
  )) |> 
  group_by(shi) |> 
  summarise(剂次数=n(),费用=sum(价格)) |> 
  janitor::adorn_totals()|> 
  openxlsx::write.xlsx('/mnt/c/Users/Administrator/Downloads/6月删除.xlsx')