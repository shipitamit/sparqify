library(dplyr)
sales <- read.csv("sales.csv")
customers <- read.csv("customers.csv")
res <- sales %>%
  inner_join(customers, by = "customer") %>%
  mutate(big = amount > 250) %>%
  group_by(segment, big) %>%
  summarise(total = sum(amount), avg_qty = mean(qty), n = n()) %>%
  arrange(segment, big)
