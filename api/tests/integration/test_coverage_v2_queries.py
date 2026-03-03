"""
Integration tests for NL2SQL coverage expansion v2 corner-case query families.
These queries target newly added intents and bilingual phrasing.
"""
import pytest

from tests.integration.conftest import find_col, numeric_col, str_col


@pytest.mark.integration
async def test_top_and_bottom_product_per_waiter(ask):
    body = await ask("What are the top-selling and bottom-selling products for each waiter?")
    results = body["results"]
    assert len(results) >= 2
    assert str_col(results[0], "waiter", "name")
    assert str_col(results[0], "product", "name")


@pytest.mark.integration
async def test_least_sold_product_per_waiter(ask):
    body = await ask("What is the least sold product for each waiter?")
    results = body["results"]
    assert len(results) >= 1
    qty = numeric_col(results[0], "qty", "quantity", "total")
    assert qty >= 0


@pytest.mark.integration
async def test_second_best_product_per_waiter(ask):
    body = await ask("What is the second highest revenue product for each waiter?")
    results = body["results"]
    assert len(results) >= 1
    assert str_col(results[0], "waiter", "name")


@pytest.mark.integration
async def test_products_sold_every_month(ask):
    body = await ask("Which products were sold in every month of the dataset?")
    results = body["results"]
    assert len(results) >= 1
    assert str_col(results[0], "product", "name")


@pytest.mark.integration
async def test_rolling_7day_revenue(ask):
    body = await ask("Show the 7-day moving average revenue by date.")
    results = body["results"]
    assert len(results) >= 2
    avg_key = find_col(results[0], "moving", "avg", "rolling")
    if avg_key is not None:
        assert float(results[0][avg_key]) == float(results[0][avg_key])


@pytest.mark.integration
async def test_waiters_with_zero_forward_sales(ask):
    body = await ask("Which waiters have zero forward-sales tickets?")
    results = body["results"]
    for row in results:
        assert str_col(row, "waiter", "name")
        forward_key = find_col(row, "forward", "ticket")
        if forward_key is not None:
            assert float(row[forward_key]) == 0.0


# Spanish variants across hard categories

@pytest.mark.integration
async def test_spanish_dual_extrema(ask):
    body = await ask("Cual es el producto mas y menos vendido por cada vendedor?")
    assert len(body["results"]) >= 2


@pytest.mark.integration
async def test_spanish_bottom_per_waiter(ask):
    body = await ask("Cual es el producto menos vendido por cada vendedor?")
    assert len(body["results"]) >= 1


@pytest.mark.integration
async def test_spanish_second_best_per_waiter(ask):
    body = await ask("Cual es el segundo producto con mayor ingreso por vendedor?")
    assert len(body["results"]) >= 1


@pytest.mark.integration
async def test_spanish_all_periods_coverage(ask):
    body = await ask("Que productos se vendieron en todos los meses del dataset?")
    assert len(body["results"]) >= 1


@pytest.mark.integration
async def test_spanish_rolling_window(ask):
    body = await ask("Muestra el promedio movil de 7 dias de ingresos.")
    assert len(body["results"]) >= 2


@pytest.mark.integration
async def test_spanish_zero_activity(ask):
    body = await ask("Que vendedores no tienen tickets de venta FCB o FCA?")
    assert isinstance(body["results"], list)
