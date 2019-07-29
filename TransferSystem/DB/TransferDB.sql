-- usage:
--   $ mysql --user=root -p
--   > create database TransferDB;
--   > ...
--   $ mysql -u Dirac -pXXX --database=TransferDB < TransferDB.sql

ALTER DATABASE CHARACTER SET "utf8";

-- for foreign key
drop table if exists TransferFileList;
drop table if exists TransferRequest;

create table TransferRequest (
  id int not null auto_increment primary key,
  username varchar(255) not null,
  index(username),
  dataset varchar(255) not null,
  srcSE varchar(255) not null,
  dstSE varchar(255) not null,
  protocol varchar(255) not null,
  submit_time datetime not null,
  status enum('new', 'transfer', 'finish') not null,
  index(status)
) ENGINE=InnoDB;

drop table if exists TransferFileList;

create table TransferFileList (
  id int not null auto_increment primary key,
  LFN varchar(255) not null,
  trans_req_id int not null,
  start_time datetime,
  finish_time datetime,
  status enum('new', 'transfer', 'finish', 'kill') not null,
  error varchar(65535),
  index(status),
  foreign key (trans_req_id) references TransferRequest (id)
) ENGINE=InnoDB;

-- 2013.03.13
-- we need give user the ability to create their own dataset.
-- the file list may be from DFC. we don't care.
drop table if exists FilesInDataSet;
drop table if exists Dataset;

create table Dataset (
  id int not null auto_increment primary key,
  name varchar(255) not null unique,
  username varchar(255) not null,
  index(username)
);

create table FilesInDataSet (
  id int not null auto_increment primary key,
  LFN varchar(255) not null,
  dataset_id int not null,
  foreign key (dataset_id) references Dataset (id)
);
