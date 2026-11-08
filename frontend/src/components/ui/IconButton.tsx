import { type ReactNode, type ButtonHTMLAttributes, forwardRef } from 'react';

export type IconButtonVariant = 'ghost' | 'secondary' | 'danger';
export type IconButtonSize = 'xs' | 'sm' | 'md';

interface IconButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: IconButtonVariant;
  size?: IconButtonSize;
  active?: boolean;
  children: ReactNode;
  title?: string;
}

const VARIANT_STYLES: Record<IconButtonVariant, string> = {
  ghost:
    'text-[var(--color-text-secondary)] hover:text-[var(--color-text-primary)] hover:bg-[var(--color-bg-field)] bg-transparent border-transparent',
  secondary:
    'text-[var(--color-text-secondary)] hover:text-[var(--color-text-primary)] hover:bg-[var(--color-bg-field-hover)] bg-[var(--color-bg-field)] border-[var(--color-border-subtle)]',
  danger:
    'text-[var(--color-danger)] hover:bg-[var(--color-danger-soft)] bg-transparent border-transparent',
};

const SIZE_STYLES: Record<IconButtonSize, string> = {
  xs: 'w-6 h-6 rounded-[var(--radius-sm)]',
  sm: 'w-7 h-7 rounded-[var(--radius-md)]',
  md: 'w-8 h-8 rounded-[var(--radius-md)]',
};

const ACTIVE_STYLE = 'ring-1 ring-[var(--color-accent)] ring-offset-1 ring-offset-[var(--color-bg-app)]';

export const IconButton = forwardRef<HTMLButtonElement, IconButtonProps>(
  (
    {
      variant = 'ghost',
      size = 'sm',
      active = false,
      children,
      className = '',
      title,
      ...props
    },
    ref
  ) => {
    const base = [
      'inline-flex items-center justify-center transition-all duration-[var(--motion-fast)]',
      'border cursor-pointer select-none',
      VARIANT_STYLES[variant],
      SIZE_STYLES[size],
      active ? ACTIVE_STYLE : '',
      'disabled:opacity-50 disabled:cursor-not-allowed disabled:pointer-events-none',
    ]
      .filter(Boolean)
      .join(' ');

    return (
      <button
        ref={ref}
        className={`${base} ${className}`}
        title={title}
        type="button"
        aria-label={title}
        {...props}
      >
        <span className="w-4 h-4 flex items-center justify-center">{children}</span>
      </button>
    );
  }
);
IconButton.displayName = 'IconButton';
