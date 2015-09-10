/*
 * MySQL - 5.6.24 
 * 
 * 数据库初始化脚本
 */

CREATE TABLE `userinfo` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `username` varchar(13) NOT NULL,
  `password` varchar(13) NOT NULL,
  `gender` varchar(6) DEFAULT NULL,
  `salary` double(8,2) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UNIQUE` (`username`)
);

INSERT INTO `userinfo` (`username`, `password`, `gender`, `salary`) VALUES('xiaomi','360','male','6500.00');
INSERT INTO `userinfo` (`username`, `password`, `gender`, `salary`) VALUES('test','test123','female','6000.00');
INSERT INTO `userinfo` (`username`, `password`, `gender`, `salary`) VALUES('dashi','123123','male','0.00');
INSERT INTO `userinfo` (`username`, `password`, `gender`, `salary`) VALUES('maxwell','nc','male','99999.00');
