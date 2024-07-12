import numpy as np
from scipy.stats import binom, binomtest
import scipy.stats as stats

def binom_test_python(x, n=None, p=0.5, alternative='two-sided', conf_level=0.95):
    # Check and process input
    if isinstance(x, list) and len(x) == 2:
        n = sum(x)
        x = x[0]
    elif isinstance(x, int) and n is not None:
        if not isinstance(n, int) or n < 1 or x > n:
            raise ValueError("'n' must be a positive integer >= 'x'")
    else:
        raise ValueError("incorrect length of 'x'")

    if not (0 <= p <= 1):
        raise ValueError("'p' must be a single number between 0 and 1")

    if not (0 < conf_level < 1):
        raise ValueError("'conf.level' must be a single number between 0 and 1")

    # Perform binomial test
    result = binomtest(x, n, p, alternative=alternative)

    # Calculate confidence intervals
    alpha = 1 - conf_level
    if alternative == 'less':
        ci = (0, stats.beta.ppf(1 - alpha, x + 1, n - x))
    elif alternative == 'greater':
        ci = (stats.beta.ppf(alpha, x, n - x + 1), 1)
    else:  # two-sided
        alpha /= 2
        ci = (stats.beta.ppf(alpha, x, n - x + 1), stats.beta.ppf(1 - alpha, x + 1, n - x))

    # Prepare result
    result_dict = {
        'statistic': x,
        'parameter': n,
        'p.value': result.pvalue,
        'conf.int': ci,
        'estimate': x / n,
        'null.value': p,
        'alternative': alternative,
        'method': 'Exact binomial test',
        'data.name': 'x and n'
    }

    return result_dict

# Example usage:
# result = binom_test_python(x=3, n=10, p=0.5, alternative='two-sided', conf_level=0.95)
# print(result)

# import scipy
# import binom_test
# result = binom_test.binom_test_python(x=30, n=100, p=0.5, alternative='two-sided', conf_level=0.95)
# print(result)
# result['conf.int']
