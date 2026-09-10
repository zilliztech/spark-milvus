# core 的 antlr4 源码目录

Plan.g4 放这里：Milvus 表达式文法，`core.expr` 的 PlanParser 由它生成，承载 R7。

antlr runtime 放哪是决策 17（docs/design/README.md）：a. core 自带并 relocate；b. Plan.g4 放共享源码，各 Spark 线用本线的 antlr 版本各生成一份。Spark 3.5 带 antlr 4.9.3，4.x 带 4.13.1，生成代码不通用，而 core 是跨线单产物。决策落地前这里只放文法文件。
