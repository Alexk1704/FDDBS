NetBeans:
- Run Build
- Run federated app or parser test app
  
  - run federated app:
    - In Netbeans with a run config
      - choose "Federated Test" and click run project (f6)
      - (does currently not work, because fed-class implementation is missing)
 
  - run parser test app:
    - from console:
      - cd .\build\classes\
      - java fdbs.app.parser.App "query"
        - like: java fdbs.app.parser.App "C:\src\Verteilte-DB\build\classes\fdbs\app\parser\queries\AssignmentExamples.sql"
    - In Netbeans with a run config
      - choose "Parser test" and click run project (f6)
