package main

import (
	"fmt"
	"testing"
)

func BenchmarkLargeCellPopulation(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s := NewSpreadsheet()
		s.AddWorksheet("Sheet1")

		for row := 1; row <= 100; row++ {
			for col := 1; col <= 26; col++ {
				addr := fmt.Sprintf("Sheet1!%c%d", 'A'+col-1, row)
				s.Set(addr, float64(row*col))
			}
		}
	}
}

func BenchmarkFormulaDependencyChain(b *testing.B) {
	s := NewSpreadsheet()
	s.AddWorksheet("Sheet1")

	s.Set("Sheet1!A1", 1.0)
	for i := 2; i <= 100; i++ {
		addr := fmt.Sprintf("Sheet1!A%d", i)
		formula := fmt.Sprintf("=A%d+1", i-1)
		s.Set(addr, formula)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Calculate()
	}
}

func BenchmarkWideDependencyFanOut(b *testing.B) {
	s := NewSpreadsheet()
	s.AddWorksheet("Sheet1")

	s.Set("Sheet1!A1", 100.0)
	for i := 2; i <= 500; i++ {
		addr := fmt.Sprintf("Sheet1!B%d", i)
		s.Set(addr, "=A1*2")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Set("Sheet1!A1", float64(i))
		s.Calculate()
	}
}

func BenchmarkLargeRangeSUM(b *testing.B) {
	s := NewSpreadsheet()
	s.AddWorksheet("Sheet1")

	for i := 1; i <= 1000; i++ {
		addr := fmt.Sprintf("Sheet1!A%d", i)
		s.Set(addr, float64(i))
	}
	s.Set("Sheet1!B1", "=SUM(A1:A1000)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Calculate()
	}
}

func BenchmarkComplexNestedFormulas(b *testing.B) {
	s := NewSpreadsheet()
	s.AddWorksheet("Sheet1")

	for i := 1; i <= 20; i++ {
		s.Set(fmt.Sprintf("Sheet1!A%d", i), float64(i))
		s.Set(fmt.Sprintf("Sheet1!B%d", i), float64(i*2))
	}

	s.Set("Sheet1!C1", "=IF(AVERAGE(A1:A20)>10, SUM(B1:B20), MAX(A1:A20))")
	s.Set("Sheet1!D1", "=ROUND(SQRT(C1)*PI(), 2)")
	s.Set("Sheet1!E1", "=IF(D1>100, MEDIAN(A1:A20), MIN(B1:B20))")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Calculate()
	}
}

func BenchmarkVolatileFunctions(b *testing.B) {
	s := NewSpreadsheet()
	s.AddWorksheet("Sheet1")

	for i := 1; i <= 50; i++ {
		addr := fmt.Sprintf("Sheet1!A%d", i)
		s.Set(addr, "=RAND()")
	}
	for i := 1; i <= 50; i++ {
		addr := fmt.Sprintf("Sheet1!B%d", i)
		formula := fmt.Sprintf("=A%d*100", i)
		s.Set(addr, formula)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Calculate()
	}
}

func BenchmarkMultiWorksheetReferences(b *testing.B) {
	s := NewSpreadsheet()
	s.AddWorksheet("Sheet1")
	s.AddWorksheet("Data")
	s.AddWorksheet("Summary")

	for i := 1; i <= 100; i++ {
		s.Set(fmt.Sprintf("Sheet1!Data!A%d", i), float64(i))
	}

	s.Set("Sheet1!Summary!A1", "=SUM(Data!A1:A100)")
	s.Set("Sheet1!Summary!B1", "=AVERAGE(Data!A1:A100)")
	s.Set("Sheet1!Summary!C1", "=MAX(Data!A1:A100)")
	s.Set("Sheet1!Summary!D1", "=MIN(Data!A1:A100)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Calculate()
	}
}

func BenchmarkCascadingUpdates(b *testing.B) {
	s := NewSpreadsheet()
	s.AddWorksheet("Sheet1")

	for row := 1; row <= 50; row++ {
		for col := 0; col < 10; col++ {
			addr := fmt.Sprintf("Sheet1!%c%d", 'A'+col, row)
			if col == 0 {
				s.Set(addr, float64(row))
			} else {
				prevCol := fmt.Sprintf("%c%d", 'A'+col-1, row)
				s.Set(addr, fmt.Sprintf("=%s*2", prevCol))
			}
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Set("Sheet1!A1", float64(i%100))
		s.Calculate()
	}
}

func BenchmarkSparseMatrix(b *testing.B) {
	s := NewSpreadsheet()
	s.AddWorksheet("Sheet1")

	for i := 1; i <= 1000; i += 10 {
		for j := 1; j <= 1000; j += 10 {
			addr := fmt.Sprintf("Sheet1!%s%d", columnToLetters(j), i)
			s.Set(addr, float64(i+j))
		}
	}

	s.Set("Sheet1!ZZ1", "=SUM(A1:ALZ1000)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Calculate()
	}
}

func BenchmarkCircularReferenceDetection(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s := NewSpreadsheet()
		s.AddWorksheet("Sheet1")

		s.Set("Sheet1!A1", "=B1+C1")
		s.Set("Sheet1!B1", "=C1+D1")
		s.Set("Sheet1!C1", "=D1+E1")
		s.Set("Sheet1!D1", "=E1+F1")
		s.Set("Sheet1!E1", "=F1+G1")
		s.Set("Sheet1!F1", "=G1+H1")
		s.Set("Sheet1!G1", "=H1+A1")
		s.Set("Sheet1!H1", "=A1")

		s.Calculate()
	}
}

func BenchmarkManySmallFormulas(b *testing.B) {
	s := NewSpreadsheet()
	s.AddWorksheet("Sheet1")

	for row := 1; row <= 100; row++ {
		s.Set(fmt.Sprintf("Sheet1!A%d", row), float64(row))
		s.Set(fmt.Sprintf("Sheet1!B%d", row), fmt.Sprintf("=A%d*2", row))
		s.Set(fmt.Sprintf("Sheet1!C%d", row), fmt.Sprintf("=B%d+A%d", row, row))
		s.Set(fmt.Sprintf("Sheet1!D%d", row), fmt.Sprintf("=C%d/2", row))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Calculate()
	}
}

func BenchmarkStringConcatenation(b *testing.B) {
	s := NewSpreadsheet()
	s.AddWorksheet("Sheet1")

	for i := 1; i <= 100; i++ {
		s.Set(fmt.Sprintf("Sheet1!A%d", i), fmt.Sprintf("text%d", i))
		s.Set(fmt.Sprintf("Sheet1!B%d", i), fmt.Sprintf(`=A%d&"-suffix"`, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Calculate()
	}
}

func BenchmarkAggregationFunctions(b *testing.B) {
	s := NewSpreadsheet()
	s.AddWorksheet("Sheet1")

	for i := 1; i <= 500; i++ {
		s.Set(fmt.Sprintf("Sheet1!A%d", i), float64(i))
	}

	s.Set("Sheet1!B1", "=SUM(A1:A500)")
	s.Set("Sheet1!B2", "=AVERAGE(A1:A500)")
	s.Set("Sheet1!B3", "=COUNT(A1:A500)")
	s.Set("Sheet1!B4", "=MAX(A1:A500)")
	s.Set("Sheet1!B5", "=MIN(A1:A500)")
	s.Set("Sheet1!B6", "=MEDIAN(A1:A500)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Calculate()
	}
}

func BenchmarkConditionalLogic(b *testing.B) {
	s := NewSpreadsheet()
	s.AddWorksheet("Sheet1")

	for i := 1; i <= 200; i++ {
		s.Set(fmt.Sprintf("Sheet1!A%d", i), float64(i))
		s.Set(fmt.Sprintf("Sheet1!B%d", i), fmt.Sprintf(`=IF(A%d>100, A%d*2, A%d/2)`, i, i, i))
		s.Set(fmt.Sprintf("Sheet1!C%d", i), fmt.Sprintf(`=AND(A%d>50, A%d<150)`, i, i))
		s.Set(fmt.Sprintf("Sheet1!D%d", i), fmt.Sprintf(`=OR(A%d<25, A%d>175)`, i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Calculate()
	}
}

func BenchmarkDirtyPropagation(b *testing.B) {
	s := NewSpreadsheet()
	s.AddWorksheet("Sheet1")

	grid := 20
	for row := 1; row <= grid; row++ {
		for col := 1; col <= grid; col++ {
			addr := fmt.Sprintf("Sheet1!%c%d", 'A'+col-1, row)
			if row == 1 && col == 1 {
				s.Set(addr, 1.0)
			} else if row == 1 {
				prevAddr := fmt.Sprintf("%c%d", 'A'+col-2, row)
				s.Set(addr, fmt.Sprintf("=%s+1", prevAddr))
			} else if col == 1 {
				prevAddr := fmt.Sprintf("%c%d", 'A'+col-1, row-1)
				s.Set(addr, fmt.Sprintf("=%s+1", prevAddr))
			} else {
				leftAddr := fmt.Sprintf("%c%d", 'A'+col-2, row)
				topAddr := fmt.Sprintf("%c%d", 'A'+col-1, row-1)
				s.Set(addr, fmt.Sprintf("=%s+%s", leftAddr, topAddr))
			}
		}
	}

	s.Calculate()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Set("Sheet1!A1", float64(i%100))
		s.Calculate()
	}
}

func columnToLetters(col int) string {
	result := ""
	for col > 0 {
		col--
		result = string(rune('A'+col%26)) + result
		col /= 26
	}
	return result
}
