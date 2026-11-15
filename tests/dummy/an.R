library(dplyr)
df <- read.csv("sales.csv")
out <- df %>% filter(amount > 100, region != "X") %>% mutate(net = amount * (1 - discount), band = ifelse(net > 500, "hi", "lo")) %>%
  group_by(region) %>% summarise(total = sum(net), n = n()) %>% arrange(desc(total))
write.csv(out, "out.csv")
