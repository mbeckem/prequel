#ifndef PREQUEL_CONTAINER_ITERATION_HPP
#define PREQUEL_CONTAINER_ITERATION_HPP

namespace prequel {

/**
 * Control values for callback based iteration.
 */
enum class iteration_control {
    /// Stop the iteration.
    stop,

    /// Move to the next element.
    next,
};

} // namespace prequel

#endif // PREQUEL_CONTAINER_ITERATION_HPP
