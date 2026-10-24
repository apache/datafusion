set datafusion.sql_parser.recursion_limit = 100;
select abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(1)))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) as deep;
set datafusion.sql_parser.recursion_limit = 5;
select abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(1)))))))))) as shallow;
