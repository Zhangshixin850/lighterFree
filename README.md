首先这是一个运行在ubuntu上面的多平台中性网格python脚本，请仔细研究脚本的逻辑和风格，不要做任何修改，将来的工作要基于这个脚本进行。



v4.0

请在lighterFree目录里重写一下lighter-robinhood平台的中性网格策略并建立Light\_grid.py，.env， lighter\_exchange.py（适配器）文件，requirements文件，不需要用ccxt包。主要改动为下单和撤单使用批量接口，并启用websocket，以符合api速率限制。

主要要求如下：

1\.其他平台完全舍弃，只保留lighter-robinhood平台。

2\.按您的建议，下单和撤单合批，尽量将接口请求合并，减少请求。

3\.按您的建议启用websocket，低频轮询

4.将SYMBOL映射为平台内的symbol格式。

5. 代码要简洁易读，减少微型辅助函数的封装，尽量扁平化。

6.readme.me文件不要动，其他都可以动。

7.精简和美化控制台的显示信息，无需显示订单ID。

8.添加账户级风控逻辑，添加两个参数：Account margin ratio limit:MMR=0.2，Account effective leverage limit.leverage=5。



