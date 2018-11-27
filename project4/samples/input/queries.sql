SELECT distinct S.A FROM Sailors S,Boats B WHERE S.A>150 and S.A<=1500 and S.A > S.B and S.A = B.E and S.B = B.D and S.A<>B.D order by S.A;
