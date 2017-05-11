/*Проверка сбалансированности решеток*/
	%let mvCheck = 1 ;
	%let mvTotal = 0;
	%let mvInpString = %str(%NRBQUOTE(&mvPatternText));
	%do %while (&mvCheck ne 0 );
		%let mvCheck = %index(%nrbquote(&mvInpString),#);
		/*в тексте шаблона найдена решетка*/
		%if &mvCheck ne 0 %then %do ;
			%let  mvPos = %eval(&mvCheck+1);
			%let mvSubstr = %substr(%nrbquote(&mvInpString),&mvPos,1);
			/*если следующий после решетки символ не # , то ошибка*/
			%if %nrbquote(&mvSubstr) ne # %then %do;
				%MA_RAISE_ERROR(%str(Unbalanced # in &mvPatternText),mvErrorNum=20750);
				%goto EXIT;
			%end;
			%let  mvPos = %eval(&mvPos+1);
			%let mvSubstr = %substr(%nrbquote(&mvInpString),&mvPos,1);
			/*если более двух решеток подряд, то ошибка*/
			%if %nrbquote(&mvSubstr) eq # %then %do;
				%MA_RAISE_ERROR(%str(Unbalanced # in &mvPatternText),mvErrorNum=20755);
				%goto EXIT;
			%end;
			%let mvInpString = %nrbquote(%substr(%nrbquote(&mvInpString),&mvPos));
			%let  mvTotal = %eval(&mvTotal+1);
		%end;
	%end;
	/*считаем количество пар решеток*/
	%let mvTotal = %sysevalf(&mvTotal/2);
	/*если количество пар решеток нечетное, то ошибка*/
	%if %sysfunc(ceil(&mvTotal)) ne &mvTotal %then %do;
		%MA_RAISE_ERROR(%str(Unbalanced # in &mvPatternText),mvErrorNum=20760);
		%goto EXIT;
	%end;
