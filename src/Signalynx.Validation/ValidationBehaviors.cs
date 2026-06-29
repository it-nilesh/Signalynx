using FluentValidation;
using FluentValidation.Results;

namespace Signalynx;

public sealed class ValidationBehavior<TRequest, TResult>
    : IPipelineBehavior<TRequest, TResult>
{
    private readonly IReadOnlyList<IValidator<TRequest>> _validators;

    public ValidationBehavior(IEnumerable<IValidator<TRequest>> validators)
    {
        _validators = validators as IReadOnlyList<IValidator<TRequest>> ?? validators.ToArray();
    }

    public async ValueTask<TResult> HandleAsync(
        TRequest request,
        RequestHandlerDelegate<TResult> next,
        CancellationToken cancellationToken = default)
    {
        if (_validators.Count == 0)
        {
            return await next().ConfigureAwait(false);
        }

        List<ValidationFailure>? failures = null;
        var context = new ValidationContext<TRequest>(request);
        for (var i = 0; i < _validators.Count; i++)
        {
            var result = await _validators[i].ValidateAsync(context, cancellationToken).ConfigureAwait(false);
            if (!result.IsValid)
            {
                (failures ??= []).AddRange(result.Errors);
            }
        }

        if (failures is { Count: > 0 })
        {
            throw new ValidationException(failures);
        }

        return await next().ConfigureAwait(false);
    }
}
