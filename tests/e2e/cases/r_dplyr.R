library(dplyr)
sales <- read.csv("sales.csv")
regions <- read.csv("regions.csv")
res <- sales %>%
  filter(qty > 2) %>%
  mutate(net = amount * 0.9) %>%
  group_by(region) %>%
  summarise(total = sum(net), n = n()) %>%
  left_join(regions, by = "region") %>%
  arrange(desc(total))
