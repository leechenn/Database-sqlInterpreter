select * from Reserves where Reserves.H > 103;
select * from Reserves R where R.G > 2;
select * from Reserves R,Sailors S,Boats B where S.C>190 and R.G>2 and S.B < B.D;
select * from Reserves,Sailors,Boats where Sailors.C>50 and Sailors.C < Boats.D;
select Reserves.G,Sailors.A from Reserves,Sailors;





