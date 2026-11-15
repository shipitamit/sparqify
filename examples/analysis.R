library(dplyr)
library(readr)

sales <- read_csv("sales.csv")
customers <- read_parquet("customers.parquet")

result <- sales %>%
  filter(amount > 100, region %in% c("EU", "US")) %>%
  mutate(total = amount * qty,
         tier = case_when(total > 1000 ~ "gold", total > 100 ~ "silver", TRUE ~ "basic"),
         share = amount / sum(amount)) %>%
  left_join(customers, by = c("customer_id" = "id")) %>%
  group_by(region, tier) %>%
  summarise(revenue = sum(total), n = n(), avg_qty = mean(qty)) %>%
  arrange(desc(revenue)) %>%
  head(10)

sales$flag <- ifelse(sales$amount > 50, "hi", "lo")
result %>% write_csv("out.csv")
nrow(sales)
x <- sales %>% mutate(l = lag(amount))
