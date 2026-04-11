create external table ab (a int, b int) stored as sqlite location '_';
create external table ac (a int, c int) stored as sqlite location '_';
create table ad (a int, d int);

insert into ab values (1,10),(2,20);
insert into ac values (1,10),(3,30),(4,40);
insert into ad values (1,10),(2,20),(4,40),(5,50);

