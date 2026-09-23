-- H2 demo 源库初始化脚本（幂等，可重复执行）
--
-- 正常情况不用手动跑：examples/h2-demo.yaml 的 JDBC URL 里带了
--   INIT=RUNSCRIPT FROM './demo/init.sql'
-- 首次连接时 H2 会自动执行本脚本建表并灌入种子数据，之后每次连接再执行一遍也安全。
-- 想彻底重来：rm -f demo/src.mv.db demo/src.trace.db，下次运行自动重建。
--
-- 需要手动执行时使用：
--   java -cp ~/.m2/repository/com/h2database/h2/2.2.224/h2-2.2.224.jar \
--        org.h2.tools.RunScript -url "jdbc:h2:./demo/src;MODE=MySQL" -user sa -password "" -script demo/init.sql
--
-- 之后即可：
--   java -jar weir-cli/target/weir-cli-*.jar check       -c examples/h2-demo.yaml
--   java -jar weir-cli/target/weir-cli-*.jar full        -c examples/h2-demo.yaml
--   java -jar weir-cli/target/weir-cli-*.jar incremental -c examples/h2-demo.yaml

CREATE TABLE IF NOT EXISTS orders (
  id          BIGINT          PRIMARY KEY,
  user_id     BIGINT          NOT NULL,
  amount      DECIMAL(10, 2)  NOT NULL,
  update_time TIMESTAMP       NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_orders_update_time ON orders (update_time);

-- 幂等写入：已存在同 id 的行不会被覆盖
INSERT INTO orders (id, user_id, amount, update_time)
SELECT 1, 10, 1.50, TIMESTAMP '2024-01-01 10:00:00'
WHERE NOT EXISTS (SELECT 1 FROM orders WHERE id = 1);

INSERT INTO orders (id, user_id, amount, update_time)
SELECT 2, 11, 2.25, TIMESTAMP '2024-01-01 10:05:00'
WHERE NOT EXISTS (SELECT 1 FROM orders WHERE id = 2);

INSERT INTO orders (id, user_id, amount, update_time)
SELECT 3, 10, 9.99, TIMESTAMP '2024-01-01 10:10:00'
WHERE NOT EXISTS (SELECT 1 FROM orders WHERE id = 3);

INSERT INTO orders (id, user_id, amount, update_time)
SELECT 4, 12, 5.00, TIMESTAMP '2024-01-01 10:15:00'
WHERE NOT EXISTS (SELECT 1 FROM orders WHERE id = 4);

INSERT INTO orders (id, user_id, amount, update_time)
SELECT 5, 13, 7.75, TIMESTAMP '2024-01-01 10:20:00'
WHERE NOT EXISTS (SELECT 1 FROM orders WHERE id = 5);
